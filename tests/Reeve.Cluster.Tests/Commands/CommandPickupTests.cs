using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Claims;
using Unit.Testing;

namespace Unit.Commands;

public sealed class CommandPickupTests : IDisposable
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private readonly FakeCommandRows rows = new();
    private readonly FakeCommandTenures tenures = new();
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly ClusterScopeSource scopes = new(new FenceSteps());
    private readonly HookedPoints points = new();
    private readonly CommandLifetime lifetime = new();
    private readonly List<CommandPickup> pickups = [];

    private static UnitKind Registered(bool convergent = false) =>
        new(Kind, typeof(object), new UnitKindOptions { Convergent = convergent }) { Keyed = true };

    private CommandPickup Pickup(
        ScriptedCommandHandler handler, InDoubtPolicy policy = InDoubtPolicy.ReRun, IEnumerable<UnitKind>? registered = null, TimeSpan? interval = null, TimeProvider? time = null,
        ILogger<CommandPickup>? logger = null)
    {
        var handlers = new CommandHandlers(new Dictionary<string, (IDurableCommandHandler, InDoubtPolicy)> { [Kind] = (handler, policy) });
        var pickup = new CommandPickup(
            rows, tenures, handlers, registered ?? [Registered()], scopes, points, wakeups, lifetime,
            Options.Create(new ClusterOptions { ReplicaId = "a", PlacementInterval = interval ?? TimeSpan.FromMinutes(5), FencedTransactionTimeout = TimeSpan.FromSeconds(1) }),
            time ?? TimeProvider.System, logger ?? NullLogger<CommandPickup>.Instance);
        pickups.Add(pickup);
        return pickup;
    }

    private static async Task PassAsync(CommandPickup pickup)
    {
        await pickup.PassAsync(CancellationToken.None);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);
    }

    public void Dispose()
    {
        foreach (var pickup in pickups)
        {
            pickup.Dispose();
        }
    }

    [Fact]
    public async Task A_pending_command_is_taken_run_outside_the_take_and_completed_under_the_tenures_token()
    {
        var command = rows.Add();
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(9, command.TakenToken);
        Assert.Equal(new byte[] { (byte)command.Id }, command.Result);
    }

    [Fact]
    public async Task One_command_at_a_time_per_key_oldest_first()
    {
        var first = rows.Add();
        var second = rows.Add();
        var third = rows.Add();
        var handler = new ScriptedCommandHandler(async (_, _) =>
        {
            await Task.Delay(20, CancellationToken.None);
            return new byte[] { 1 };
        });
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal([first.Id, second.Id, third.Id], handler.Ran);
        Assert.Equal(1, handler.MaxConcurrent);
    }

    [Fact]
    public async Task Keys_run_side_by_side_and_a_key_this_replica_does_not_own_is_left_alone()
    {
        var mine1 = rows.Add(key: "k1");
        var mine2 = rows.Add(key: "k2");
        var foreign = rows.Add(key: "k3");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            if (Interlocked.Increment(ref started) == 2)
            {
                gate.SetResult();
            }

            await gate.Task.WaitAsync(Bounds.Safety, ct);
            return new byte[] { 1 };
        });
        tenures.Start(Kind, "k1", 9);
        tenures.Start(Kind, "k2", 9);

        await PassAsync(Pickup(handler));

        Assert.Equal(2, handler.MaxConcurrent);
        Assert.Equal(CommandState.Done, mine1.State);
        Assert.Equal(CommandState.Done, mine2.State);
        Assert.Equal(CommandState.Pending, foreign.State);
    }

    [Fact]
    public async Task I33_a_take_whose_commit_was_cancelled_before_it_returned_does_not_run_the_handler_although_the_call_succeeded()
    {
        var command = rows.Add();
        rows.TakeOverride = () => TakeAction.Unknown;
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Taken, command.State);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task I33_the_next_poll_runs_a_command_it_reads_taken_under_the_own_token_without_a_new_take()
    {
        var command = rows.Add();
        var override_ = true;
        rows.TakeOverride = () => override_ ? TakeAction.Unknown : null;
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await PassAsync(pickup);
        Assert.Empty(handler.Ran);

        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal([command.Id], rows.Takes);
    }

    [Fact]
    public async Task I33_the_next_poll_takes_again_a_command_whose_cancelled_take_did_not_land()
    {
        var command = rows.Add();
        var first = true;
        rows.TakeOverride = () =>
        {
            if (first)
            {
                first = false;
                rows.Table[0].State = CommandState.Pending;
                rows.Table[0].TakenToken = null;
                return TakeAction.Unknown;
            }

            return null;
        };
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await PassAsync(pickup);
        Assert.Empty(handler.Ran);

        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal([command.Id, command.Id], rows.Takes);
    }

    [Fact]
    public async Task I1_a_replica_that_stops_serving_between_the_takes_commit_and_the_handler_start_runs_no_handler()
    {
        var command = rows.Add();
        var handler = new ScriptedCommandHandler();
        var owner = tenures.Start(Kind, Key, 9).Owner as StubOwner;
        points.On(CommandsFaultPoints.AfterTake.Name, () => owner!.StopServing());

        await PassAsync(Pickup(handler));

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Taken, command.State);
        Assert.Equal(9, command.TakenToken);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task I1_the_serving_check_comes_after_the_after_take_point_and_directly_before_the_handler_starts()
    {
        rows.Add();
        var order = new List<string>();
        var owner = (StubOwner)tenures.Start(Kind, Key, 9).Owner;
        var startsAtPoint = -1;
        points.On(CommandsFaultPoints.AfterTake.Name, () => startsAtPoint = owner.Starts);
        var handler = new ScriptedCommandHandler((_, _) =>
        {
            order.Add($"handler:{owner.Starts}");
            return Task.FromResult<ReadOnlyMemory<byte>>(default);
        });

        await PassAsync(Pickup(handler));

        Assert.Equal(1, startsAtPoint);
        Assert.Equal(["handler:2"], order);
    }

    [Fact]
    public async Task I1_the_run_path_of_a_command_read_taken_under_the_own_token_starts_no_handler_on_a_replica_that_stopped_serving()
    {
        var command = rows.Add(token: 9, state: CommandState.Taken);
        var handler = new ScriptedCommandHandler();
        var owner = (StubOwner)tenures.Start(Kind, Key, 9).Owner;
        owner.StopServing();

        await PassAsync(Pickup(handler));

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Taken, command.State);
        Assert.Equal(9, command.TakenToken);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task S1_the_handler_gets_the_context_of_the_tenure_it_acts_for()
    {
        rows.Add();
        DurableCommand? seen = null;
        var handler = new ScriptedCommandHandler((command, _) =>
        {
            seen = command;
            return Task.FromResult<ReadOnlyMemory<byte>>(default);
        });
        var tenure = tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Same(tenure.Context, seen!.Owner);
        Assert.Equal(9, seen.Owner.Token);
        Assert.Equal((Kind, Key), (seen.Owner.Kind, seen.Owner.Key));
    }

    [Fact]
    public async Task I1_the_run_path_checks_that_the_replica_serves_before_it_starts_the_handler()
    {
        rows.Add(token: 9, state: CommandState.Taken);
        var owner = (StubOwner)tenures.Start(Kind, Key, 9).Owner;
        var startsAtHandler = -1;
        var handler = new ScriptedCommandHandler((_, _) =>
        {
            startsAtHandler = owner.Starts;
            return Task.FromResult<ReadOnlyMemory<byte>>(default);
        });

        await PassAsync(Pickup(handler));

        Assert.Equal(1, startsAtHandler);
    }

    [Fact]
    public async Task A_completion_whose_commit_was_cancelled_before_it_returned_is_not_run_again_and_the_row_decides()
    {
        var command = rows.Add();
        rows.CompleteOverride = () => CompleteAction.Unknown;
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);

        await PassAsync(pickup);
        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
    }

    [Fact]
    public async Task A_completion_that_got_no_answer_is_made_again_with_the_remembered_result_and_the_handler_does_not_run_again()
    {
        var command = rows.Add();
        var fail = true;
        rows.CompleteOverride = () => fail ? throw new DbFailure("no answer") : null;
        var handler = new ScriptedCommandHandler((_, _) => Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 42 }));
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await PassAsync(pickup);
        Assert.Equal(CommandState.Taken, command.State);

        fail = false;
        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(new byte[] { 42 }, command.Result);
    }

    [Fact]
    public async Task A_handler_that_throws_completes_the_command_done_with_the_error_and_the_key_goes_on()
    {
        var bad = rows.Add();
        var later = rows.Add();
        var handler = new ScriptedCommandHandler((command, _) => command.Id == bad.Id ? throw new InvalidOperationException("boom") : Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 7 }));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal([bad.Id, later.Id], handler.Ran);
        Assert.Equal(CommandState.Done, bad.State);
        Assert.Equal("boom", bad.Error);
        Assert.Null(bad.Result);
        Assert.Equal(9, bad.TakenToken);
        Assert.Equal(CommandState.Done, later.State);
        Assert.Null(later.Error);
    }

    [Fact]
    public async Task A_handler_that_throws_is_completed_through_the_fenced_completion_under_the_taking_token()
    {
        var bad = rows.Add();
        var handler = new ScriptedCommandHandler((_, _) => throw new InvalidOperationException("boom"));
        var owner = (StubOwner)tenures.Start(Kind, Key, 9).Owner;

        await PassAsync(Pickup(handler));

        Assert.Equal([bad.Id], rows.Completions);
        Assert.Equal(CommandState.Done, bad.State);
        Assert.Equal(3, owner.Starts);
    }

    [Fact]
    public async Task A_handler_that_cancels_itself_with_no_stop_requested_is_a_throw_and_completes_with_an_error()
    {
        var bad = rows.Add();
        var handler = new ScriptedCommandHandler((_, _) => throw new OperationCanceledException("its own timeout"));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal(CommandState.Done, bad.State);
        Assert.Equal("The handler was cancelled.", bad.Error);
    }

    [Fact]
    public async Task A_handler_error_carries_the_message_of_the_exception_the_handler_threw_as_Ask_does()
    {
        var bad = rows.Add();
        var handler = new ScriptedCommandHandler((_, _) => throw new InvalidOperationException("outer", new TimeoutException("inner")));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal("outer", bad.Error);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_handler_error_the_database_cannot_store_is_normalised_so_the_key_goes_on(bool loneSurrogate)
    {
        var message = loneSurrogate ? "half a pair " + '\uD83D' : "bad value 'a\0b'";
        var stored = loneSurrogate ? "half a pair \uFFFD" : "bad value 'a\uFFFDb'";
        var bad = rows.Add();
        var later = rows.Add();
        var handler = new ScriptedCommandHandler((command, _) => command.Id == bad.Id ? throw new FormatException(message) : Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 7 }));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal([bad.Id, later.Id], handler.Ran);
        Assert.Equal(CommandState.Done, bad.State);
        Assert.Equal(stored, bad.Error);
        Assert.Equal(CommandState.Done, later.State);
        Assert.Null(later.Error);
    }

    [Fact]
    public async Task A_handler_whose_exception_has_no_message_completes_with_an_error_naming_its_type_and_is_not_read_as_completed()
    {
        var bad = rows.Add();
        var handler = new ScriptedCommandHandler((_, _) => throw new SilentException());
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal(CommandState.Done, bad.State);
        Assert.Equal("The handler threw SilentException.", bad.Error);
        Assert.Null(bad.Result);
    }

    [Fact]
    public async Task A_handler_error_longer_than_the_cap_is_cut()
    {
        var bad = rows.Add();
        var handler = new ScriptedCommandHandler((_, _) => throw new InvalidOperationException(new string('x', 100_000)));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal(CommandText.MaxErrorLength, bad.Error!.Length);
    }

    [Fact]
    public async Task A_completion_the_database_refuses_for_its_content_is_made_again_once_with_a_fixed_error_and_the_key_goes_on()
    {
        var command = rows.Add();
        var later = rows.Add();
        rows.CompleteCheck = outcome =>
        {
            if (outcome.Error == "boom")
            {
                throw new PostgresException("invalid input", "ERROR", "ERROR", "22P02");
            }
        };
        var handler = new ScriptedCommandHandler((c, _) => c.Id == command.Id ? throw new InvalidOperationException("boom") : Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 7 }));
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);

        await PassAsync(pickup);
        Assert.Equal(CommandState.Taken, command.State);
        await PassAsync(pickup);

        Assert.Equal([command.Id, later.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(CommandText.Unstorable, command.Error);
        Assert.Equal(CommandState.Done, later.State);
    }

    [Fact]
    public async Task A_returned_result_whose_completion_the_database_refuses_for_its_content_ends_the_command_with_the_fixed_error()
    {
        var command = rows.Add();
        rows.CompleteCheck = outcome =>
        {
            if (outcome.Error is null)
            {
                throw new PostgresException("invalid input", "ERROR", "ERROR", "22000");
            }
        };
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(new ScriptedCommandHandler());

        await PassAsync(pickup);
        await PassAsync(pickup);

        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(CommandText.Unstorable, command.Error);
    }

    [Fact]
    public async Task A_completion_that_fails_on_the_connection_is_made_again_with_the_same_outcome()
    {
        var command = rows.Add();
        var failures = 2;
        rows.CompleteCheck = _ =>
        {
            if (failures-- > 0)
            {
                throw new PostgresException("failover", "FATAL", "FATAL", "57P01");
            }
        };
        var handler = new ScriptedCommandHandler((_, _) => throw new InvalidOperationException("boom"));
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);

        await PassAsync(pickup);
        await PassAsync(pickup);
        Assert.Equal(CommandState.Taken, command.State);
        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal("boom", command.Error);
    }

    [Fact]
    public async Task A_handler_that_throws_because_the_tenure_stopped_leaves_the_command_taken_for_the_next_owner()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("stopped midway");
            }

            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);

        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Null(command.Error);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task A_handler_that_throws_because_the_host_stopped_leaves_the_command_taken()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        using var host = new CancellationTokenSource();
        await pickup.PassAsync(host.Token);
        await started.Task.WaitAsync(Bounds.Safety);

        await host.CancelAsync();
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Null(command.Error);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task A_completion_with_an_error_that_got_no_answer_is_made_again_from_the_remembered_error_and_the_handler_does_not_run_again()
    {
        var command = rows.Add();
        var fail = true;
        rows.CompleteOverride = () => fail ? throw new DbFailure("no answer") : null;
        var handler = new ScriptedCommandHandler((_, _) => throw new InvalidOperationException("boom"));
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await PassAsync(pickup);
        Assert.Equal(CommandState.Taken, command.State);

        fail = false;
        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal("boom", command.Error);
    }

    [Fact]
    public async Task A_completion_is_nudged_to_the_waiters_on_the_completed_channel_and_not_to_the_pickup()
    {
        rows.Add();
        rows.Add();
        var targets = new List<string?>();
        wakeups.Subscribe(WakeupKind.CommandNudge, w => targets.Add(w.Target));
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(new ScriptedCommandHandler((c, _) => c.Id == 1 ? throw new InvalidOperationException("x") : Task.FromResult<ReadOnlyMemory<byte>>(default))));

        Assert.Equal([CommandNudges.Completed(Kind, Key), CommandNudges.Completed(Kind, Key)], targets);
        Assert.NotEqual(Kind, CommandNudges.Completed(Kind, Key));
    }

    [Fact]
    public async Task A_tenure_that_stops_under_the_handler_leaves_the_command_taken_and_completes_nothing()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);

        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);
        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task ReRun_a_command_taken_by_an_older_tenure_is_taken_again_under_the_new_token_and_run_and_the_key_goes_on()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var pending = rows.Add();
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler, InDoubtPolicy.ReRun));

        Assert.Equal([old.Id, pending.Id], handler.Ran);
        Assert.Equal([old.Id], rows.Retakes);
        Assert.Equal(CommandState.Done, old.State);
        Assert.Equal(9, old.TakenToken);
        Assert.Equal(CommandState.Done, pending.State);
    }

    [Fact]
    public async Task Complete_a_command_taken_by_an_older_tenure_becomes_in_doubt_and_runs_nothing_and_the_key_goes_on()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var pending = rows.Add();
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler, InDoubtPolicy.Complete));

        Assert.Equal([old.Id], rows.Marks);
        Assert.Equal(CommandState.InDoubt, old.State);
        Assert.Equal(5, old.TakenToken);
        Assert.Equal([pending.Id], handler.Ran);
        Assert.Equal(CommandState.Done, pending.State);
    }

    [Fact]
    public async Task A_command_taken_by_an_older_tenure_is_classified_on_each_poll_not_only_on_acquisition()
    {
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, InDoubtPolicy.Complete);
        await PassAsync(pickup);

        var late = rows.Add(token: 5, state: CommandState.Taken);
        await PassAsync(pickup);

        Assert.Equal(CommandState.InDoubt, late.State);
        Assert.Empty(handler.Ran);
    }

    [Fact]
    public async Task I33_a_retake_whose_commit_was_cancelled_before_it_returned_runs_no_handler_and_the_next_poll_runs_it_as_the_own_tenures()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var first = true;
        rows.RetakeOverride = () =>
        {
            var result = first ? TakeAction.Unknown : (TakeAction?)null;
            first = false;
            return result;
        };
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, InDoubtPolicy.ReRun);

        await PassAsync(pickup);
        Assert.Empty(handler.Ran);
        Assert.Equal(9, old.TakenToken);

        await PassAsync(pickup);

        Assert.Equal([old.Id], handler.Ran);
        Assert.Equal([old.Id], rows.Retakes);
        Assert.Equal(CommandState.Done, old.State);
    }

    [Fact]
    public async Task I33_a_retake_whose_cancelled_commit_did_not_land_is_classified_again_at_the_next_poll()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var first = true;
        rows.RetakeOverride = () =>
        {
            if (first)
            {
                first = false;
                old.TakenToken = 5;
                return TakeAction.Unknown;
            }

            return null;
        };
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, InDoubtPolicy.ReRun);
        await PassAsync(pickup);
        Assert.Empty(handler.Ran);
        old.TakenToken = 5;

        await PassAsync(pickup);

        Assert.Equal([old.Id, old.Id], rows.Retakes);
        Assert.Equal([old.Id], handler.Ran);
    }

    [Fact]
    public async Task I33_an_in_doubt_verdict_whose_commit_was_cancelled_before_it_returned_runs_nothing_and_the_next_poll_reads_the_row()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var pending = rows.Add();
        rows.MarkOverride = () => MarkAction.Unknown;
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, InDoubtPolicy.Complete);

        await PassAsync(pickup);

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Pending, pending.State);
        rows.MarkOverride = null;

        await PassAsync(pickup);

        Assert.Equal(CommandState.InDoubt, old.State);
        Assert.Equal([pending.Id], handler.Ran);
        Assert.Equal([old.Id], rows.Marks);
    }

    [Fact]
    public async Task I1_a_retaken_command_starts_its_handler_only_after_a_serving_check()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var handler = new ScriptedCommandHandler();
        var owner = (StubOwner)tenures.Start(Kind, Key, 9).Owner;
        points.On(CommandsFaultPoints.AfterTake.Name, () => owner.StopServing());

        await PassAsync(Pickup(handler, InDoubtPolicy.ReRun));

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Taken, old.State);
        Assert.Equal(9, old.TakenToken);
    }

    [Fact]
    public async Task A_retake_that_matches_no_row_because_a_successor_took_the_command_meanwhile_runs_nothing()
    {
        var old = rows.Add(token: 5, state: CommandState.Taken);
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, InDoubtPolicy.ReRun);
        rows.AfterRead = () => old.TakenToken = 7;

        await PassAsync(pickup);

        Assert.Empty(handler.Ran);
        Assert.Equal(7, old.TakenToken);
        Assert.Equal(CommandState.Taken, old.State);
    }

    [Fact]
    public async Task The_handler_runs_in_a_scope_of_its_own_and_not_in_the_flow_that_polls()
    {
        rows.Add();
        IClusterScope? seen = null;
        IClusterScope? outer = null;
        var handler = new ScriptedCommandHandler((_, _) =>
        {
            seen = scopes.Current;
            return Task.FromResult<ReadOnlyMemory<byte>>(default);
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        using var caller = scopes.Begin(ClusterScopeKind.Background);
        outer = scopes.Current;

        await PassAsync(pickup);

        Assert.NotNull(seen);
        Assert.NotSame(outer, seen);
    }

    [Fact]
    public async Task The_fault_points_are_reached_between_the_take_and_the_handler_and_before_the_completion()
    {
        var command = rows.Add();
        var order = new List<string>();
        points.On(CommandsFaultPoints.AfterTake.Name, () => order.Add($"after-take:{command.State}"));
        points.On(CommandsFaultPoints.BeforeComplete.Name, () => order.Add($"before-complete:{command.State}"));
        var handler = new ScriptedCommandHandler((_, _) =>
        {
            order.Add("handler");
            return Task.FromResult<ReadOnlyMemory<byte>>(default);
        });
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(handler));

        Assert.Equal(["after-take:Taken", "handler", "before-complete:Taken"], order);
    }

    [Fact]
    public void A_handler_for_a_convergent_kind_is_refused_at_construction()
    {
        Assert.Throws<InvalidOperationException>(() => Pickup(new ScriptedCommandHandler(), registered: [Registered(convergent: true)]));
    }

    [Fact]
    public void A_handler_for_a_kind_that_is_not_registered_on_this_replica_is_refused_at_construction()
    {
        Assert.Throws<InvalidOperationException>(() => Pickup(new ScriptedCommandHandler(), registered: []));
        Assert.Throws<InvalidOperationException>(() => Pickup(new ScriptedCommandHandler(), registered: [new UnitKind("other", typeof(object), new UnitKindOptions())]));
    }

    [Fact]
    public async Task A_take_writes_the_token_of_the_tenure_that_took_it_and_a_completion_needs_the_same_one()
    {
        var command = rows.Add();
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 7);

        await PassAsync(Pickup(handler));

        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(7, command.TakenToken);
    }

    [Fact]
    public async Task A_take_that_outlasts_the_librarys_bound_is_an_unknown_outcome_that_runs_no_handler_and_the_next_poll_decides()
    {
        var command = rows.Add();
        var time = new FakeTimeProvider();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rows.TakeGate = async ct =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
        };
        var handler = new ScriptedCommandHandler();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, time: time);

        await pickup.PassAsync(CancellationToken.None);
        await entered.Task.WaitAsync(Bounds.Safety);
        time.Advance(TimeSpan.FromSeconds(3));
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Empty(handler.Ran);
        Assert.Equal(CommandState.Pending, command.State);

        rows.TakeGate = null;
        await PassAsync(pickup);
        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
    }

    [Fact]
    public async Task A_completion_that_outlasts_the_librarys_bound_is_an_unknown_outcome_and_the_next_poll_completes_again_without_running_the_handler_again()
    {
        var command = rows.Add();
        var time = new FakeTimeProvider();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rows.CompleteGate = async ct =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
        };
        var handler = new ScriptedCommandHandler((_, _) => Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 42 }));
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, time: time);

        await pickup.PassAsync(CancellationToken.None);
        await entered.Task.WaitAsync(Bounds.Safety);
        time.Advance(TimeSpan.FromSeconds(3));
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);
        Assert.Equal(CommandState.Taken, command.State);

        rows.CompleteGate = null;
        await PassAsync(pickup);

        Assert.Equal([command.Id], handler.Ran);
        Assert.Equal(CommandState.Done, command.State);
        Assert.Equal(new byte[] { 42 }, command.Result);
    }

    [Fact]
    public async Task A_handler_the_tenures_stop_abandoned_blocks_the_keys_next_tenure_until_it_has_ended()
    {
        var first = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (command, _) =>
        {
            if (command.Id == first.Id)
            {
                started.SetResult();
                await release.Task;
            }

            return new byte[] { 1 };
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);
        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        first.State = CommandState.InDoubt;
        var next = rows.Add();
        tenures.Start(Kind, Key, 11);
        await PassAsync(pickup);

        Assert.Equal([first.Id], handler.Ran);
        Assert.Equal(CommandState.Pending, next.State);
        Assert.Equal(1, handler.MaxConcurrent);

        await pickup.StartAsync(CancellationToken.None);
        await Until(() => pickup.Polls >= 1);
        Assert.Equal(CommandState.Pending, next.State);
        release.SetResult();
        await Until(() => next.State == CommandState.Done);
        await pickup.StopAsync(CancellationToken.None);
        Assert.Equal([first.Id, next.Id], handler.Ran);
        Assert.Equal(1, handler.MaxConcurrent);
        Assert.Equal(11, next.TakenToken);
    }

    [Fact]
    public async Task The_host_stopping_cancels_a_handler_the_tenure_abandoned()
    {
        rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                cancelled.SetResult();
                throw;
            }

            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        using var host = new CancellationTokenSource();
        await pickup.PassAsync(host.Token);
        await started.Task.WaitAsync(Bounds.Safety);
        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        await host.CancelAsync();

        await cancelled.Task.WaitAsync(Bounds.Safety);
    }

    [Fact]
    public async Task A_command_nudge_for_the_kind_wakes_the_poll_before_its_interval()
    {
        var handler = new ScriptedCommandHandler();
        var pickup = Pickup(handler, interval: TimeSpan.FromMinutes(10));
        await pickup.StartAsync(CancellationToken.None);
        await Until(() => pickup.Polls >= 1);
        var command = rows.Add();
        tenures.Start(Kind, Key, 9);
        await Until(() => command.State == CommandState.Done);

        var second = rows.Add();
        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, Kind));
        await Until(() => second.State == CommandState.Done);

        await pickup.StopAsync(CancellationToken.None);
        Assert.Equal([command.Id, second.Id], handler.Ran);
    }

    [Fact]
    public async Task A_nudge_for_another_kind_does_not_wake_the_poll()
    {
        var pickup = Pickup(new ScriptedCommandHandler(), interval: TimeSpan.FromMinutes(10));
        await pickup.StartAsync(CancellationToken.None);
        await Until(() => pickup.Polls >= 1);

        await wakeups.PublishAsync(Wakeup.Nudge(WakeupKind.CommandNudge, "other"));
        await Task.Delay(100);

        Assert.Equal(1, pickup.Polls);
        await pickup.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task A_key_acquired_with_pending_commands_is_taken_on_acquisition_without_waiting_for_the_poll()
    {
        var command = rows.Add();
        var handler = new ScriptedCommandHandler();
        var pickup = Pickup(handler, interval: TimeSpan.FromMinutes(10));
        await pickup.StartAsync(CancellationToken.None);
        await Until(() => pickup.Polls >= 1);
        Assert.Equal(CommandState.Pending, command.State);

        tenures.Start(Kind, Key, 9);

        await Until(() => command.State == CommandState.Done);
        await pickup.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task A_racing_take_that_the_unique_index_refused_gets_its_own_log_line_and_not_the_generic_step_failure()
    {
        rows.Add();
        rows.TakeGate = _ => throw new PostgresException("duplicate key", "ERROR", "ERROR", PostgresErrorCodes.UniqueViolation, constraintName: "command_one_taken");
        var log = new CapturingLog();
        tenures.Start(Kind, Key, 9);

        await PassAsync(Pickup(new ScriptedCommandHandler(), logger: log));

        Assert.Contains(log.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("command_one_taken", StringComparison.Ordinal));
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("the command step failed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_handler_that_throws_after_the_stop_leaves_the_exception_in_the_log()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("defect found at shutdown");
            }

            return default;
        });
        var log = new CapturingLog();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, logger: log);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);

        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        await Until(() => log.Entries.Any(e => e.Exception is InvalidOperationException { Message: "defect found at shutdown" } or AggregateException { InnerException.Message: "defect found at shutdown" }));
        Assert.Equal(CommandState.Taken, command.State);
    }

    [Fact]
    public async Task An_abandoned_handler_that_later_faults_leaves_its_exception_in_the_log()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
            throw new InvalidOperationException("ignored the token, then threw");
        });
        var log = new CapturingLog();
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, logger: log);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);
        tenures.Stop(Kind, Key);
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        release.SetResult();
        await Until(() => log.Entries.Any(e => e.Level == LogLevel.Warning && e.Exception is not null));

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task A_handler_that_returns_after_the_stop_was_requested_leaves_the_command_taken()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, _) =>
        {
            started.SetResult();
            await release.Task;
            return new byte[] { 1 };
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);

        tenures.Stop(Kind, Key);
        release.SetResult();
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task A_handler_whose_dependency_cancels_on_ApplicationStopping_and_throws_leaves_the_command_taken()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, _) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, lifetime.ApplicationStopping);
            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler);
        await pickup.PassAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);

        lifetime.StopApplication();
        await pickup.WhenStepsEndAsync().WaitAsync(Bounds.Safety);

        Assert.Equal(CommandState.Taken, command.State);
        Assert.Null(command.Error);
        Assert.Empty(rows.Completions);
    }

    [Fact]
    public async Task ApplicationStopping_stops_the_polls_loop_and_cancels_a_running_handlers_token()
    {
        var command = rows.Add();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedCommandHandler(async (_, ct) =>
        {
            started.SetResult();
            ct.Register(() => cancelled.SetResult());
            await Task.Delay(Timeout.Infinite, ct);
            return default;
        });
        tenures.Start(Kind, Key, 9);
        var pickup = Pickup(handler, interval: TimeSpan.FromMilliseconds(20), time: TimeProvider.System);
        await pickup.StartAsync(CancellationToken.None);
        await started.Task.WaitAsync(Bounds.Safety);

        lifetime.StopApplication();

        await cancelled.Task.WaitAsync(Bounds.Safety);
        await pickup.StopAsync(CancellationToken.None).WaitAsync(Bounds.Safety);
        Assert.Equal(CommandState.Taken, command.State);
    }

    private sealed class CapturingLog : ILogger<CommandPickup>
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
            {
                Entries.Add((logLevel, formatter(state, exception), exception));
            }
        }
    }

    private static async Task Until(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Bounds.Safety;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the condition did not come true");
            await Task.Delay(10);
        }
    }

    private sealed class SilentException : Exception
    {
        public override string Message => null!;
    }
}
