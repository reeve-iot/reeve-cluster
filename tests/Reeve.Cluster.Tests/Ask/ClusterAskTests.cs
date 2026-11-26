using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Membership;
using Unit.Testing;
using static Unit.Ask.ScriptedTransport;

namespace Unit.Ask;

public sealed class ClusterAskTests
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private static readonly byte[] Request = [9];

    private readonly SignallingTimeProvider time = new();
    private readonly ScriptedDirectory directory = new();
    private readonly ScriptedTransport transport = new();
    private readonly ScriptedInbound inbound = new();
    private readonly MemberIdentity self = new(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
    private readonly Guid other1 = Guid.NewGuid();
    private readonly Guid other2 = Guid.NewGuid();

    private ClusterAsk Create() => new(
        directory,
        inbound,
        transport,
        self,
        [],
        Options.Create(new ClusterOptions { ReplicaId = "a" }),
        time,
        NullLogger<ClusterAsk>.Instance);

    private AskOwner Remote(Guid process, long token, string replica = "b") => new(replica, process, token);

    private AskOwner Mine(long token) => new("a", self.ProcessId, token);

    private Task<(AskResult Result, TimeSpan Moved)> RunAsync(Func<Task<AskResult>> ask, TimeSpan limit) =>
        time.RunDelaysAsync(ask, limit);

    [Fact]
    public async Task The_serving_owner_is_asked_with_kind_key_and_token()
    {
        directory.Then(Remote(other1, 7));
        transport.Then(Answers(4, 5));

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(new byte[] { 4, 5 }, result.Response.ToArray());
        var (target, delivery) = Assert.Single(transport.Deliveries);
        Assert.Equal(new AskTarget("b", other1), target);
        Assert.Equal((Kind, Key, (long?)7), (delivery.Kind, delivery.Key, delivery.Token));
        Assert.Equal(Request, delivery.Payload.ToArray());
        Assert.Empty(inbound.Received);
    }

    [Fact]
    public async Task A_caller_that_is_the_owner_dispatches_locally()
    {
        directory.Then(Mine(7));
        inbound.Answering(4);

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Empty(transport.Deliveries);
        var delivery = Assert.Single(inbound.Received);
        Assert.Equal((Kind, Key, (long?)7), (delivery.Kind, delivery.Key, delivery.Token));
    }

    [Fact]
    public async Task An_owner_of_this_replica_id_but_another_process_is_a_remote_target()
    {
        directory.Then(new AskOwner("a", other1, 7));
        transport.Then(Answers(1));

        await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(new AskTarget("a", other1), Assert.Single(transport.Deliveries).Target);
        Assert.Empty(inbound.Received);
    }

    [Fact]
    public async Task A_decline_resolves_again_and_the_new_owner_answers()
    {
        directory.Then(Remote(other1, 7)).Then(Remote(other2, 9, "c"));
        transport.Then(Declines).Then(Answers(1));

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(2, directory.OwnerResolutions);
        Assert.Equal(
            [(new AskTarget("b", other1), 7L), (new AskTarget("c", other2), 9L)],
            transport.Deliveries.Select(d => (d.Target, d.Delivery.Token!.Value)).ToArray());
    }

    [Fact]
    public async Task A_second_decline_is_NotTaken_and_there_is_no_third_delivery()
    {
        directory.Then(Remote(other1, 7)).Then(Remote(other2, 9, "c")).Then(Remote(other2, 11, "c"));
        transport.Then(Declines).Then(Declines).Then(Answers(1));

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Equal(2, directory.OwnerResolutions);
        Assert.Equal(2, transport.Deliveries.Count);
    }

    [Fact]
    public async Task A_local_decline_is_resolved_again_like_a_remote_one()
    {
        directory.Then(Mine(7)).Then(Remote(other1, 9));
        inbound.Declining();
        transport.Then(Answers(1));

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Single(inbound.Received);
        Assert.Equal(9L, Assert.Single(transport.Deliveries).Delivery.Token);
    }

    [Fact]
    public async Task A_decline_whose_second_resolution_finds_no_owner_is_NoOwner_after_the_wait()
    {
        directory.Then(Remote(other1, 7));
        transport.Then(Declines);

        var (result, moved) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(3)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(3), moved);
        Assert.Single(transport.Deliveries);
    }

    [Theory]
    [InlineData("NotServing")]
    [InlineData("Stopping")]
    public async Task A_decline_for_an_owner_that_is_leaving_waits_for_another_owner_and_delivers_once_more(string reason)
    {
        directory.Then(Remote(other1, 7)).Then(Remote(other1, 7)).Then(Remote(other1, 7)).Then(Remote(other1, 7)).Then(Remote(other2, 9, "c"));
        transport.Then(DeclinesFor(Enum.Parse<DeclineReason>(reason))).Then(Answers(1));

        var (result, _) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(
            [(new AskTarget("b", other1), 7L), (new AskTarget("c", other2), 9L)],
            transport.Deliveries.Select(d => (d.Target, d.Delivery.Token!.Value)).ToArray());
    }

    [Fact]
    public async Task A_decline_for_an_owner_that_is_leaving_and_stays_named_is_NoOwner_after_the_wait_with_one_delivery()
    {
        directory.Then(Remote(other1, 7));
        directory.Otherwise = Remote(other1, 7);
        transport.Then(DeclinesFor(DeclineReason.Stopping)).Then(Answers(1));

        var (result, moved) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(3)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(3), moved);
        Assert.Single(transport.Deliveries);
    }

    [Fact]
    public async Task A_decline_for_a_tenure_not_started_delivers_to_the_same_owner_after_one_poll()
    {
        directory.Otherwise = Remote(other1, 7);
        transport.Then(DeclinesFor(DeclineReason.Absent)).Then(Answers(1));

        var (result, moved) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(ClusterAsk.OwnerPollInterval, moved);
        Assert.Equal(2, transport.Deliveries.Count);
    }

    [Fact]
    public async Task A_second_decline_after_the_wait_is_NotTaken_and_there_are_two_deliveries_in_all()
    {
        directory.Otherwise = Remote(other1, 7);
        transport.Then(DeclinesFor(DeclineReason.Absent)).Then(DeclinesFor(DeclineReason.Absent)).Then(Answers(1));

        var (result, _) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Equal(2, transport.Deliveries.Count);
    }

    [Theory]
    [InlineData("NoHandler")]
    [InlineData("WaitEnded")]
    public async Task A_decline_that_no_second_delivery_could_change_is_NotTaken_after_one_delivery_and_no_wait(string reason)
    {
        directory.Otherwise = Remote(other1, 7);
        transport.Then(DeclinesFor(Enum.Parse<DeclineReason>(reason))).Then(Answers(1));

        var (result, moved) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Equal(TimeSpan.Zero, moved);
        Assert.Single(transport.Deliveries);
    }

    [Fact]
    public async Task A_key_with_no_serving_owner_is_NoOwner_when_the_timeout_passes()
    {
        var (result, moved) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(3)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(3), moved);
        Assert.Empty(transport.Deliveries);
        Assert.True(directory.OwnerResolutions > 1);
    }

    [Fact]
    public async Task An_owner_that_appears_within_the_wait_is_asked()
    {
        directory.Then(null).Then(null).Then(null).Then(Remote(other1, 7));
        transport.Then(Answers(1));

        var (result, _) = await RunAsync(() => Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30)), TimeSpan.FromSeconds(10));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Equal(4, directory.OwnerResolutions);
    }

    [Fact]
    public async Task The_wait_for_an_owner_is_at_most_the_failover_bound_and_the_rest_of_the_timeout_goes_to_the_answer()
    {
        var options = new ClusterOptions
        {
            ReplicaId = "a",
            LeaseTtl = TimeSpan.FromSeconds(3),
            PlacementInterval = TimeSpan.FromSeconds(1),
            FencedTransactionTimeout = TimeSpan.FromSeconds(1),
        };
        var (result, moved) = await RunAsync(() => new ClusterAsk(directory, inbound, transport, self, [], Options.Create(options), time, NullLogger<ClusterAsk>.Instance)
            .AskAsync(Kind, Key, Request, TimeSpan.FromMinutes(10)), TimeSpan.FromSeconds(20));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(7), moved);
    }

    [Fact]
    public async Task The_wait_after_a_decline_is_also_at_most_the_failover_bound()
    {
        var options = new ClusterOptions
        {
            ReplicaId = "a",
            LeaseTtl = TimeSpan.FromSeconds(3),
            PlacementInterval = TimeSpan.FromSeconds(1),
            FencedTransactionTimeout = TimeSpan.FromSeconds(1),
        };
        directory.Otherwise = Remote(other1, 7);
        transport.Then(DeclinesFor(DeclineReason.Stopping)).Then(Answers(1));
        var (result, moved) = await RunAsync(() => new ClusterAsk(directory, inbound, transport, self, [], Options.Create(options), time, NullLogger<ClusterAsk>.Instance)
            .AskAsync(Kind, Key, Request, TimeSpan.FromMinutes(10)), TimeSpan.FromSeconds(30));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(7), moved);
        Assert.Single(transport.Deliveries);
    }

    [Fact]
    public async Task The_wait_for_an_owner_uses_the_failover_bound_of_a_kind_registered_on_the_caller()
    {
        var options = new ClusterOptions
        {
            ReplicaId = "a",
            LeaseTtl = TimeSpan.FromSeconds(3),
            PlacementInterval = TimeSpan.FromSeconds(1),
            FencedTransactionTimeout = TimeSpan.FromSeconds(1),
        };
        var registered = new Reeve.Cluster.Owned.UnitKind(Kind, typeof(IOwnedUnit), new UnitKindOptions { StartTime = TimeSpan.FromSeconds(10) });
        var (result, moved) = await RunAsync(() => new ClusterAsk(directory, inbound, transport, self, [registered], Options.Create(options), time, NullLogger<ClusterAsk>.Instance)
            .AskAsync(Kind, Key, Request, TimeSpan.FromMinutes(10)), TimeSpan.FromSeconds(30));

        Assert.Equal(AskOutcome.NoOwner, result.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(17), moved);
    }

    [Fact]
    public async Task A_caller_that_cancels_while_waiting_for_an_owner_gets_NotTaken()
    {
        using var cancel = new CancellationTokenSource();
        var ask = Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(30), cancel.Token);
        await Task.Delay(10);
        await cancel.CancelAsync();

        Assert.Equal(AskOutcome.NotTaken, (await ask.WaitAsync(TimeSpan.FromSeconds(30))).Outcome);
    }

    [Fact]
    public async Task A_resolution_the_database_does_not_answer_is_NotTaken()
    {
        directory.Fails = new InvalidOperationException("no database");

        var result = await Create().AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Empty(transport.Deliveries);
    }

    [Fact]
    public async Task A_replica_with_no_row_is_NotTaken_at_once_and_nothing_is_delivered()
    {
        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Empty(transport.Deliveries);
        Assert.Empty(inbound.Received);
    }

    [Fact]
    public async Task A_draining_row_is_NotTaken_at_once_and_nothing_is_delivered()
    {
        directory.Member("b", new AskMemberRow(other1, MemberRowState.Draining));

        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Empty(transport.Deliveries);
        Assert.Equal(1, directory.MemberResolutions);
    }

    [Theory]
    [InlineData(MemberRowState.Joining)]
    [InlineData(MemberRowState.Active)]
    [InlineData(MemberRowState.Fenced)]
    public async Task A_row_in_any_other_state_is_delivered_to_its_process_without_a_key_or_a_token(MemberRowState state)
    {
        directory.Member("b", new AskMemberRow(other1, state));
        transport.Then(Answers(1));

        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        var (target, delivery) = Assert.Single(transport.Deliveries);
        Assert.Equal(new AskTarget("b", other1), target);
        Assert.Equal((null, null, null), (delivery.Kind, delivery.Key, delivery.Token));
    }

    [Fact]
    public async Task A_named_request_with_a_take_bound_carries_it_to_the_transport_and_one_without_carries_none()
    {
        directory.Member("b", new AskMemberRow(other1, MemberRowState.Active));
        transport.Then(Answers(1));

        await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(1));
        await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(2, transport.TakeBounds.Count);
        Assert.InRange(transport.TakeBounds[0]!.Value, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(1));
        Assert.Null(transport.TakeBounds[1]);
    }

    [Fact]
    public async Task The_take_bound_counts_from_the_call_so_the_row_read_spends_it()
    {
        directory.Member("b", new AskMemberRow(other1, MemberRowState.Active));
        transport.Then(Answers(1));
        directory.OnResolve = () => time.Advance(TimeSpan.FromMilliseconds(600));

        await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(1));

        Assert.Equal(TimeSpan.FromMilliseconds(400), Assert.Single(transport.TakeBounds));
    }

    [Fact]
    public async Task A_take_bound_the_row_read_spent_delivers_nothing_and_is_NotTaken()
    {
        directory.Member("b", new AskMemberRow(other1, MemberRowState.Active));
        directory.OnResolve = () => time.Advance(TimeSpan.FromMilliseconds(1200));

        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(1));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Empty(transport.Deliveries);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_take_bound_that_is_not_positive_is_refused(int milliseconds)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(milliseconds)));
        Assert.Empty(transport.Deliveries);
    }

    [Fact]
    public async Task A_named_request_that_is_declined_is_NotTaken_and_the_row_is_not_read_again()
    {
        directory.Member("b", new AskMemberRow(other1, MemberRowState.Active));
        transport.Then(Declines).Then(Answers(1));

        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Single(transport.Deliveries);
        Assert.Equal(1, directory.MemberResolutions);
    }

    [Fact]
    public async Task A_named_request_to_this_process_dispatches_locally_in_any_row_state()
    {
        directory.Member("a", new AskMemberRow(self.ProcessId, MemberRowState.Fenced));
        inbound.Answering(3);

        var result = await Create().AskMemberAsync("a", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.Empty(transport.Deliveries);
        Assert.Single(inbound.Received);
    }

    [Fact]
    public async Task A_row_the_database_does_not_answer_for_is_NotTaken()
    {
        directory.Fails = new InvalidOperationException("no database");

        var result = await Create().AskMemberAsync("b", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public async Task A_timeout_that_is_not_positive_and_finite_is_refused(int milliseconds)
    {
        var timeout = milliseconds < 0 ? Timeout.InfiniteTimeSpan : TimeSpan.Zero;

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Create().AskAsync(Kind, Key, Request, timeout));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Create().AskMemberAsync("b", Request, timeout));
        Assert.Equal(0, directory.OwnerResolutions + directory.MemberResolutions);
    }
}
