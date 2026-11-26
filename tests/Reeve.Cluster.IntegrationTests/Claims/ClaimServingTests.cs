using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Claims;

public sealed class ClaimServingTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public ClaimServingTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task I16_a_replica_that_does_not_serve_grants_nothing_and_writes_no_row()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        a.Serving = false;

        var result = await a.Claims.ClaimAsync("k", Minute);

        Assert.IsType<ClaimResult.NotServing>(result);
        Assert.Equal(0, await rig.CountAsync());
    }

    [Fact]
    public async Task A_replica_that_does_not_serve_does_not_take_over_an_ended_claim()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var holder = await rig.AddReplicaAsync("holder");
        var a = await rig.AddReplicaAsync("a");
        var first = await holder.WinAsync("k");
        await rig.EndLeaseAsync("holder");
        a.Serving = false;

        var result = await a.Claims.ClaimAsync("k", Minute);

        Assert.IsType<ClaimResult.NotServing>(result);
        Assert.Equal(first, (await rig.RowAsync("k"))!.Token);
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_insert_across_fence_start_grants_nothing()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.GrantBeforeInsert);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await hold.Arrived.WaitAsync(ClaimsRig.Calm);
        a.Serving = false;
        hold.Release();

        Assert.IsType<ClaimResult.NotServing>(await claim.WaitAsync(ClaimsRig.Calm));
        Assert.Equal(0, await rig.CountAsync());
    }

    [Fact]
    public async Task I1_a_process_paused_before_its_takeover_across_fence_start_takes_nothing_over()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var holder = await rig.AddReplicaAsync("holder");
        var a = await rig.AddReplicaAsync("a");
        var first = await holder.WinAsync("k");
        await rig.EndLeaseAsync("holder");
        var hold = a.Points.Hold(ClaimsFaultPoints.GrantBeforeTakeover);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await hold.Arrived.WaitAsync(ClaimsRig.Calm);
        a.Serving = false;
        hold.Release();

        Assert.IsType<ClaimResult.NotServing>(await claim.WaitAsync(ClaimsRig.Calm));
        Assert.Equal(first, (await rig.RowAsync("k"))!.Token);
    }

    [Fact]
    public async Task I1_a_process_paused_between_a_conflict_and_its_read_across_fence_start_takes_nothing_over()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var holder = await rig.AddReplicaAsync("holder");
        var a = await rig.AddReplicaAsync("a");
        var first = await holder.WinAsync("k");
        await rig.EndLeaseAsync("holder");
        var hold = a.Points.Hold(ClaimsFaultPoints.GrantBeforeRead);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await hold.Arrived.WaitAsync(ClaimsRig.Calm);
        a.Serving = false;
        hold.Release();

        Assert.IsType<ClaimResult.NotServing>(await claim.WaitAsync(ClaimsRig.Calm));
        Assert.Equal(first, (await rig.RowAsync("k"))!.Token);
    }

    [Fact]
    public async Task A_grant_that_has_committed_is_a_grant_whatever_happens_to_the_process_before_its_effect()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var hold = a.Points.Hold(ClaimsFaultPoints.GrantAfterCommit);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await hold.Arrived.WaitAsync(ClaimsRig.Calm);
        Assert.NotNull(await rig.RowAsync("k"));
        a.Serving = false;
        hold.Release();
        var token = Assert.IsType<ClaimResult.Won>(await claim.WaitAsync(ClaimsRig.Calm)).Token;

        await using var connection = await rig.Db.OpenAsync();
        await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));
    }

    [Fact]
    public async Task I1_a_claim_fenced_begin_on_a_replica_that_does_not_serve_is_refused_before_it_touches_the_connection()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        a.Serving = false;
        await using var connection = await rig.Db.OpenAsync();

        var refused = await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));

        Assert.Null(refused.InnerException);
        Assert.True(await rig.CanLockAsync("k", "FOR NO KEY UPDATE"));
        await using var next = await connection.BeginTransactionAsync();
    }

    [Fact]
    public async Task I1_a_process_paused_after_its_grant_across_fence_start_cannot_begin_its_effect()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        var checksBefore = a.Checks;

        a.Serving = false;
        await Assert.ThrowsAsync<ClaimLostException>(() => a.Claims.BeginClaimFencedTransactionAsync("k", token, connection));

        Assert.Equal(checksBefore + 1, a.Checks);
    }

    [Fact]
    public async Task A_grant_that_inserts_runs_the_serving_check_once()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");

        await a.WinAsync("k");

        Assert.Equal(1, a.Checks);
    }

    [Fact]
    public async Task A_replica_that_serves_still_loses_to_a_valid_claim_without_a_takeover_check()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var holder = await rig.AddReplicaAsync("holder");
        var a = await rig.AddReplicaAsync("a");
        await holder.WinAsync("k");

        var result = await a.Claims.ClaimAsync("k", Minute);

        Assert.Equal(new ClaimResult.Lost("holder"), result);
        Assert.Equal(1, a.Checks);
    }

    [Fact]
    public async Task Release_does_not_need_a_serving_replica_and_does_not_check_it()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        a.Serving = false;

        Assert.True(await a.Claims.ReleaseAsync("k", token));
        Assert.Null(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task A_PostgresException_of_a_statement_is_not_swallowed_by_the_serving_check()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await rig.ExecAsync("DROP TABLE reeve_cluster.claim");

        var thrown = await Assert.ThrowsAsync<PostgresException>(() => a.Claims.ClaimAsync("k", Minute));

        Assert.Equal(PostgresErrorCodes.UndefinedTable, thrown.SqlState);
    }
}
