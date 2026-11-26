using System.Globalization;
using Int.Log;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class FloorRaceTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public FloorRaceTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync()
    {
        db = await MemberDatabase.CreateAsync(postgres);

        await db.ExecuteAsync("""
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, version, protocol, process_id)
            VALUES ('new', 1, 'active', clock_timestamp() + interval '10 minutes', '2.0.0', 2, gen_random_uuid())
            """);
    }

    public async Task DisposeAsync() => await db.DisposeAsync();

    private async Task<ActivationRequest> JoinOldAsync(string version = "1.0.0", int protocol = 1)
    {
        var identity = new MemberIdentity(Guid.NewGuid(), version, protocol, []);
        var answer = await db.Store.JoinAsync(new JoinRequest("r1", TimeSpan.FromMinutes(10), identity), CancellationToken.None);
        return new ActivationRequest("r1", answer!.Value.Epoch, version, protocol);
    }

    private Task<ActivationAnswer> ActivateAsync(ActivationRequest request, IFaultPoints? points = null) =>
        db.Activation(points).ActivateAsync(request, () => true, CancellationToken.None);

    private Task<bool> RaiseAsync(bool version, IFaultPoints? points = null) => version
        ? db.VersionGate(db.Floors(points)).RequireVersionAsync("2.0.0")
        : db.VersionGate(db.Floors(points)).RequireProtocolAsync(2);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task I14_a_raise_that_starts_while_an_old_member_activates_waits_and_is_refused_by_the_member_it_finds_active(bool version)
    {
        var request = await JoinOldAsync();
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.ActivateBeforeUpdate);
        var activation = ActivateAsync(request, probe);
        await probe.Arrived.WaitAsync(Wait);

        var raise = RaiseAsync(version);
        await db.UntilALockWaitsAsync();
        Assert.False(raise.IsCompleted, "the raise must wait for the change-log lock the activation holds");
        probe.Release();

        Assert.Equal(ActivationOutcome.Committed, (await activation.WaitAsync(Wait)).Outcome);
        Assert.False(await raise.WaitAsync(Wait));
        Assert.Equal("active", (await db.ReadRowAsync("r1")).State);
        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((0, (string?)null), (floors.ProtocolFloor, floors.VersionFloor));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task I14_an_old_member_that_activates_while_a_raise_holds_the_lock_waits_and_is_refused_by_the_floor(bool version)
    {
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.FloorRaiseAfterMembersRead);
        var raise = RaiseAsync(version, probe);
        await probe.Arrived.WaitAsync(Wait);
        var request = await JoinOldAsync().WaitAsync(Wait);

        var activation = ActivateAsync(request);
        await db.UntilALockWaitsAsync();
        Assert.False(activation.IsCompleted, "the activation must wait for the change-log lock the raise holds");
        probe.Release();

        Assert.True(await raise.WaitAsync(Wait));
        var refused = await activation.WaitAsync(Wait);
        Assert.Equal(version ? ActivationOutcome.RefusedByVersionFloor : ActivationOutcome.RefusedByProtocolFloor, refused.Outcome);
        Assert.Equal("joining", (await db.ReadRowAsync("r1")).State);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task I14_a_raise_cancelled_after_it_read_the_members_writes_nothing_and_the_queued_old_member_activates(bool version, bool beforeCommit)
    {
        using var cancel = new CancellationTokenSource();
        var probe = new PointProbe();
        probe.Hold(beforeCommit ? MembershipFaultPoints.FloorRaiseBeforeCommit : MembershipFaultPoints.FloorRaiseAfterMembersRead);
        var gate = db.VersionGate(db.Floors(probe));
        var raise = version ? gate.RequireVersionAsync("2.0.0", cancel.Token) : gate.RequireProtocolAsync(2, cancel.Token);
        await probe.Arrived.WaitAsync(Wait);
        var request = await JoinOldAsync().WaitAsync(Wait);
        var activation = ActivateAsync(request);
        await db.UntilALockWaitsAsync();
        Assert.False(activation.IsCompleted, "the activation must wait for the change-log lock the raise holds");

        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => raise.WaitAsync(Wait));
        Assert.Equal(ActivationOutcome.Committed, (await activation.WaitAsync(Wait)).Outcome);
        Assert.Equal("active", (await db.ReadRowAsync("r1")).State);
        var floors = await db.Floors().ReadAsync(CancellationToken.None);
        Assert.Equal((0, (string?)null), (floors.ProtocolFloor, floors.VersionFloor));
    }

    [Fact]
    public async Task I14_a_member_at_the_new_floor_activates_after_the_raise_that_refused_an_old_one()
    {
        var probe = new PointProbe();
        probe.Hold(MembershipFaultPoints.FloorRaiseBeforeCommit);
        var raise = RaiseAsync(version: false, probe);
        await probe.Arrived.WaitAsync(Wait);
        var current = await JoinOldAsync("2.1.0", protocol: 2);
        var activation = ActivateAsync(current);
        await db.UntilALockWaitsAsync();
        probe.Release();

        Assert.True(await raise.WaitAsync(Wait));
        Assert.Equal(ActivationOutcome.Committed, (await activation.WaitAsync(Wait)).Outcome);
    }

    [Fact]
    public async Task I14_a_raise_that_found_the_old_member_joining_is_refused_and_the_member_activates()
    {
        var request = await JoinOldAsync();

        Assert.False(await RaiseAsync(version: false).WaitAsync(Wait));
        Assert.Equal(ActivationOutcome.Committed, (await ActivateAsync(request).WaitAsync(Wait)).Outcome);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task I14_a_raise_and_an_old_members_join_and_activation_end_with_exactly_one_refused(bool version)
    {
        const int Rounds = 40;
        for (var round = 0; round < Rounds; round++)
        {
            await db.ExecuteAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'r1'");
            await db.ExecuteAsync("UPDATE reeve_cluster.cluster SET protocol_floor = 0, version_floor = NULL");
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var jitter = round % 5;

            var raise = Task.Run(async () =>
            {
                await start.Task;
                await Task.Delay(TimeSpan.FromMilliseconds(jitter * 2));
                return await RaiseAsync(version);
            });
            var member = Task.Run(async () =>
            {
                await start.Task;
                await Task.Delay(TimeSpan.FromMilliseconds((4 - jitter) * 2));
                var request = await JoinOldAsync();
                return await ActivateAsync(request);
            });
            start.SetResult();
            var raiseAnswer = await raise.WaitAsync(Wait);
            var activation = await member.WaitAsync(Wait);

            var memberState = (await db.ReadRowAsync("r1")).State;
            var floors = await db.Floors().ReadAsync(CancellationToken.None);
            if (raiseAnswer)
            {
                Assert.Equal(
                    version ? ActivationOutcome.RefusedByVersionFloor : ActivationOutcome.RefusedByProtocolFloor,
                    activation.Outcome);
                Assert.Equal("joining", memberState);
                Assert.Equal(version ? "2.0.0" : "2", version ? floors.VersionFloor : floors.ProtocolFloor.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Assert.Equal(ActivationOutcome.Committed, activation.Outcome);
                Assert.Equal("active", memberState);
                Assert.Equal((0, (string?)null), (floors.ProtocolFloor, floors.VersionFloor));
            }
        }
    }
}
