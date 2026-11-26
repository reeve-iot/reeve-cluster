using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Wakeup;

namespace Unit.Commands;

public sealed class ClusterCommandsTests
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private static readonly byte[] Payload = [1, 2, 3];

    private readonly FakeCommandRows rows = new();
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly List<string> nudged = [];
    private readonly FakeTimeProvider time = new();
    private readonly ClusterCommands commands;

    public ClusterCommandsTests()
    {
        CommandHandlerRegistration[] registrations =
        [
            new(Kind, typeof(ScriptedCommandHandler), InDoubtPolicy.ReRun),
            new("other", typeof(ScriptedCommandHandler), InDoubtPolicy.ReRun),
        ];
        commands = new ClusterCommands(
            rows, registrations, wakeups, Options.Create(new ClusterOptions { ReplicaId = "a", FencedTransactionTimeout = TimeSpan.FromSeconds(1) }), time,
            NullLogger<ClusterCommands>.Instance);
        wakeups.Subscribe(WakeupKind.CommandNudge, w => nudged.Add(w.Target!));
    }

    [Fact]
    public async Task A_new_command_is_written_pending_and_nudges_the_owner()
    {
        var result = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        Assert.Equal(DurableSendOutcome.Sent, result.Outcome);
        Assert.Equal(DurableCommandState.Pending, result.State);
        var row = Assert.Single(rows.Table);
        Assert.Equal(CommandState.Pending, row.State);
        Assert.Equal(Payload, row.Payload);
        Assert.Equal([Kind], nudged);
    }

    [Fact]
    public async Task A_key_or_idempotency_key_the_table_cannot_hold_is_refused_before_any_statement()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => commands.SendDurableAsync(Kind, "k\0", Payload, "i-1"));
        await Assert.ThrowsAsync<ArgumentException>(() => commands.SendDurableAsync(Kind, Key, Payload, "i" + '\uD800'));
        await Assert.ThrowsAsync<ArgumentException>(() => commands.SendDurableAsync(Kind, "k" + '\uDC00', Payload, "i-1"));

        Assert.Empty(rows.Table);
        Assert.Empty(nudged);
    }

    [Fact]
    public async Task A_payload_holding_any_bytes_is_accepted()
    {
        var bytes = new byte[] { 0, 0xFF, 0xD8, 0x00, 0x80 };

        await commands.SendDurableAsync(Kind, Key, bytes, "i-1");

        Assert.Equal(bytes, Assert.Single(rows.Table).Payload);
    }

    [Fact]
    public async Task Sending_the_same_key_and_payload_again_returns_the_command_that_exists()
    {
        var first = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        var again = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        Assert.Equal(DurableSendOutcome.Existing, again.Outcome);
        Assert.Equal(first.CommandId, again.CommandId);
        Assert.Single(rows.Table);
    }

    [Fact]
    public async Task The_state_of_an_existing_command_is_reported_and_a_finished_one_is_not_nudged()
    {
        var first = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");
        rows.Table[0].State = CommandState.Done;
        nudged.Clear();

        var again = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        Assert.Equal(DurableSendOutcome.Existing, again.Outcome);
        Assert.Equal(DurableCommandState.Done, again.State);
        Assert.Equal(first.CommandId, again.CommandId);
        Assert.Empty(nudged);
    }

    [Fact]
    public async Task A_different_payload_under_the_same_key_is_an_idempotency_conflict_and_writes_nothing()
    {
        await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        var result = await commands.SendDurableAsync(Kind, Key, new byte[] { 9 }, "i-1");

        Assert.Equal(DurableSendOutcome.IdempotencyConflict, result.Outcome);
        Assert.Equal(Payload, Assert.Single(rows.Table).Payload);
    }

    [Fact]
    public async Task A_payload_of_the_same_length_under_the_same_key_is_an_idempotency_conflict_and_writes_nothing()
    {
        await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        var result = await commands.SendDurableAsync(Kind, Key, new byte[] { 1, 2, 4 }, "i-1");

        Assert.Equal(DurableSendOutcome.IdempotencyConflict, result.Outcome);
        Assert.Equal(Payload, Assert.Single(rows.Table).Payload);
    }

    [Fact]
    public async Task The_same_key_in_another_key_or_kind_is_another_command()
    {
        await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        await commands.SendDurableAsync(Kind, "k2", Payload, "i-1");
        await commands.SendDurableAsync("other", Key, Payload, "i-1");

        Assert.Equal(3, rows.Table.Count);
    }

    [Fact]
    public async Task An_expired_command_sent_again_with_its_payload_is_pending_again()
    {
        var first = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");
        rows.Table[0].State = CommandState.Expired;
        nudged.Clear();

        var again = await commands.SendDurableAsync(Kind, Key, Payload, "i-1");

        Assert.Equal(DurableSendOutcome.Resent, again.Outcome);
        Assert.Equal(first.CommandId, again.CommandId);
        Assert.Equal(CommandState.Pending, rows.Table[0].State);
        Assert.Equal([Kind], nudged);
    }

    [Fact]
    public async Task An_expired_command_sent_again_with_another_payload_is_a_conflict_and_stays_expired()
    {
        await commands.SendDurableAsync(Kind, Key, Payload, "i-1");
        rows.Table[0].State = CommandState.Expired;

        var result = await commands.SendDurableAsync(Kind, Key, new byte[] { 9 }, "i-1");

        Assert.Equal(DurableSendOutcome.IdempotencyConflict, result.Outcome);
        Assert.Equal(CommandState.Expired, rows.Table[0].State);
    }

    [Fact]
    public async Task A_send_with_no_idempotency_key_is_refused()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => commands.SendDurableAsync(Kind, Key, Payload, string.Empty));
    }

    [Fact]
    public async Task A_send_to_a_kind_with_no_handler_is_refused_and_writes_nothing()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => commands.SendDurableAsync("nobody", Key, Payload, "i-1"));

        Assert.Empty(rows.Table);
    }

    [Fact]
    public async Task A_send_whose_statement_outlasts_the_librarys_bound_throws_as_an_unknown_outcome_though_the_callers_token_never_cancels()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rows.InsertGate = async ct =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
        };

        var sending = commands.SendDurableAsync(Kind, Key, Payload, "i-1", CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        time.Advance(TimeSpan.FromSeconds(3));

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sending);
        Assert.Contains("same idempotency key", thrown.Message, StringComparison.Ordinal);
        Assert.Empty(nudged);
    }

    [Fact]
    public async Task A_send_whose_statement_returned_after_the_bound_ran_out_is_an_unknown_outcome_too()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rows.InsertGate = async _ =>
        {
            entered.SetResult();
            await release.Task;
        };

        var sending = commands.SendDurableAsync(Kind, Key, Payload, "i-1");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        time.Advance(TimeSpan.FromSeconds(3));
        release.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sending);
    }

    [Fact]
    public void A_handler_is_registered_once_per_kind_with_a_defined_policy()
    {
        var services = new ServiceCollection().AddClusterCommandHandler<ScriptedCommandHandler>(Kind, InDoubtPolicy.Complete);

        Assert.Throws<ArgumentException>(() => services.AddClusterCommandHandler<ScriptedCommandHandler>(Kind, InDoubtPolicy.ReRun));
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddClusterCommandHandler<ScriptedCommandHandler>(Kind, (InDoubtPolicy)7));
        var registration = Assert.Single(services.Select(d => d.ImplementationInstance).OfType<CommandHandlerRegistration>());
        Assert.Equal(InDoubtPolicy.Complete, registration.InDoubt);
    }
}
