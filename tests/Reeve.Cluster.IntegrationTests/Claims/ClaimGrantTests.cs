using System.Transactions;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Claims;

public sealed class ClaimGrantTests : IClassFixture<PostgresFixture>
{
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public ClaimGrantTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    [Fact]
    public async Task A_grant_on_a_free_key_is_won_and_writes_the_holders_row()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");

        var result = await a.Claims.ClaimAsync("k", Minute);

        var token = Assert.IsType<ClaimResult.Won>(result).Token;
        var row = await rig.RowAsync("k");
        Assert.Equal(new ClaimRow("a", a.Epoch, token, Valid: true), row);
        Assert.True(await rig.ValidAsync("k", token));
    }

    [Fact]
    public async Task A_claim_on_a_key_with_a_valid_claim_is_lost_to_its_holder_and_changes_nothing()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.WinAsync("k");
        var before = await rig.RowAsync("k");

        Assert.Equal(new ClaimResult.Lost("a"), await b.Claims.ClaimAsync("k", Minute));
        Assert.Equal(new ClaimResult.Lost("a"), await a.Claims.ClaimAsync("k", Minute));

        Assert.Equal(before, await rig.RowAsync("k"));
        Assert.True(await rig.ValidAsync("k", token));
    }

    [Fact]
    public async Task A_losing_grant_waits_for_nobody_behind_the_holders_open_claim_fenced_transaction()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, TimeSpan.FromMinutes(5));
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await using var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", token, connection);

        var read = b.Points.Hold(ClaimsFaultPoints.GrantBeforeRead);
        var grant = b.Claims.ClaimAsync("k", Minute);
        await read.Arrived.WaitAsync(Poll.Bound);
        read.Release();

        Assert.Equal(new ClaimResult.Lost("a"), await grant.WaitAsync(Poll.Bound));
    }

    [Fact]
    public async Task Tokens_are_random_version_4_uuids_and_do_not_spend_the_ownership_token_sequence()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var sequenceBefore = await rig.Db.LastTokenAsync();

        var tokens = new List<Guid>();
        for (var i = 0; i < 20; i++)
        {
            tokens.Add(await a.WinAsync($"k{i}"));
        }

        Assert.Equal(20, tokens.Distinct().Count());
        Assert.All(tokens, t => Assert.Equal(4, t.Version));
        Assert.Equal(sequenceBefore, await rig.Db.LastTokenAsync());
    }

    [Fact]
    public async Task A_claim_whose_time_to_live_has_passed_is_taken_over_under_a_new_token()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k", TimeSpan.FromMilliseconds(100));
        await rig.UntilInvalidAsync("k", first);

        var second = await b.WinAsync("k");

        Assert.NotEqual(first, second);
        Assert.Equal(new ClaimRow("b", b.Epoch, second, Valid: true), await rig.RowAsync("k"));
        Assert.False(await rig.ValidAsync("k", first));
        Assert.True(await rig.ValidAsync("k", second));
    }

    [Theory]
    [InlineData("lease")]
    [InlineData("fenced")]
    [InlineData("draining")]
    [InlineData("joining")]
    [InlineData("epoch")]
    [InlineData("deleted")]
    public async Task A_claim_is_taken_over_once_its_holders_member_row_is_no_longer_active_with_a_valid_lease_of_the_same_epoch(string end)
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k");
        Assert.Equal(new ClaimResult.Lost("a"), await b.Claims.ClaimAsync("k", Minute));

        switch (end)
        {
            case "lease":
                await rig.EndLeaseAsync("a");
                break;
            case "epoch":
                await rig.ExecAsync("UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'a'");
                break;
            case "deleted":
                await rig.ExecAsync("DELETE FROM reeve_cluster.member WHERE replica_id = 'a'");
                break;
            default:
                await rig.SetStateAsync("a", end);
                break;
        }

        Assert.False(await rig.ValidAsync("k", first));
        var second = await b.WinAsync("k");

        Assert.NotEqual(first, second);
        Assert.Equal("b", (await rig.RowAsync("k"))!.Holder);
    }

    [Fact]
    public async Task A_replica_that_rejoined_under_a_new_epoch_takes_over_its_own_earlier_claim()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var before = await rig.AddReplicaAsync("a");
        var first = await before.WinAsync("k");
        await rig.ExecAsync("UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'a'");
        var epoch = await rig.ScalarAsync<long>("SELECT epoch FROM reeve_cluster.member WHERE replica_id = 'a'");

        var after = new Replica(rig, "a", epoch);
        var second = await after.WinAsync("k");

        Assert.NotEqual(first, second);
        Assert.Equal(epoch, (await rig.RowAsync("k"))!.Epoch);
    }

    [Fact]
    public async Task A_release_by_token_ends_the_claim_and_the_key_is_claimed_again_at_once()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var b = await rig.AddReplicaAsync("b");
        var first = await a.WinAsync("k");

        Assert.True(await a.Claims.ReleaseAsync("k", first));

        Assert.Null(await rig.RowAsync("k"));
        Assert.False(await rig.ValidAsync("k", first));
        var second = await b.WinAsync("k");
        Assert.NotEqual(first, second);
    }

    [Fact]
    public async Task A_release_with_another_token_or_of_an_absent_key_changes_nothing()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        var before = await rig.RowAsync("k");

        Assert.False(await a.Claims.ReleaseAsync("k", Guid.NewGuid()));
        Assert.False(await a.Claims.ReleaseAsync("absent", token));

        Assert.Equal(before, await rig.RowAsync("k"));
    }

    [Fact]
    public async Task A_release_inside_an_open_claim_fenced_scope_of_this_process_is_refused_and_goes_through_once_it_ends()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();

        await using (var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", token, connection))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => a.Claims.ReleaseAsync("k", token));
            Assert.NotNull(await rig.RowAsync("k"));
            await fenced.CommitAsync();
        }

        Assert.True(await a.Claims.ReleaseAsync("k", token).WaitAsync(ClaimsRig.Calm));
        Assert.Null(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task A_release_of_one_key_is_not_refused_by_an_open_scope_of_another()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        var held = await a.WinAsync("held");
        var other = await a.WinAsync("other");
        await using var connection = await rig.Db.OpenAsync();
        await using var fenced = await a.Claims.BeginClaimFencedTransactionAsync("held", held, connection);

        Assert.True(await a.Claims.ReleaseAsync("other", other).WaitAsync(ClaimsRig.Calm));
    }

    [Fact]
    public async Task The_grant_is_a_transaction_of_its_own_and_never_enlists_in_the_callers_ambient_transaction()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");

        Guid token;
        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            token = await a.WinAsync("k");
        }

        Assert.NotNull(await rig.RowAsync("k"));
        Assert.True(await rig.ValidAsync("k", token));
    }

    [Fact]
    public async Task A_claim_needs_a_key_and_a_positive_time_to_live()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => a.Claims.ClaimAsync("k", TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentNullException>(() => a.Claims.ClaimAsync(null!, Minute));
        Assert.Equal(0, await rig.CountAsync());
    }

    [Fact]
    public async Task I16_of_many_replicas_claiming_one_key_at_once_exactly_one_wins_and_the_rest_lose_to_it()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var replicas = new List<Replica>();
        for (var i = 0; i < 8; i++)
        {
            replicas.Add(await rig.AddReplicaAsync($"r{i}"));
        }

        for (var round = 0; round < 5; round++)
        {
            var key = $"k{round}";
            var start = new TaskCompletionSource();
            var calls = replicas.Select(async r =>
            {
                await start.Task;
                return (r.Id, Result: await r.Claims.ClaimAsync(key, Minute));
            }).ToList();
            start.SetResult();
            var results = await Task.WhenAll(calls).WaitAsync(ClaimsRig.Calm);

            var won = Assert.Single(results, x => x.Result is ClaimResult.Won);
            Assert.All(results.Where(x => x.Id != won.Id), x => Assert.Equal(new ClaimResult.Lost(won.Id), x.Result));
            Assert.Equal(((ClaimResult.Won)won.Result).Token, (await rig.RowAsync(key))!.Token);
        }
    }

    [Fact]
    public async Task I16_of_many_replicas_taking_over_one_ended_claim_at_once_exactly_one_wins()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var holder = await rig.AddReplicaAsync("holder");
        var replicas = new List<Replica>();
        for (var i = 0; i < 8; i++)
        {
            replicas.Add(await rig.AddReplicaAsync($"r{i}"));
        }

        await holder.WinAsync("k");
        await rig.EndLeaseAsync("holder");

        var start = new TaskCompletionSource();
        var calls = replicas.Select(async r =>
        {
            await start.Task;
            return (r.Id, Result: await r.Claims.ClaimAsync("k", Minute));
        }).ToList();
        start.SetResult();
        var results = await Task.WhenAll(calls).WaitAsync(ClaimsRig.Calm);

        var won = Assert.Single(results, x => x.Result is ClaimResult.Won);
        Assert.Equal(won.Id, (await rig.RowAsync("k"))!.Holder);
        Assert.All(results.Where(x => x.Id != won.Id), x => Assert.IsType<ClaimResult.Lost>(x.Result));
    }

    [Fact]
    public async Task A_database_error_other_than_the_claims_two_is_thrown_as_it_is()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres);
        var a = await rig.AddReplicaAsync("a");
        await rig.ExecAsync("ALTER TABLE reeve_cluster.claim ADD CONSTRAINT no_forbidden CHECK (key <> 'forbidden')");

        var thrown = await Assert.ThrowsAsync<PostgresException>(() => a.Claims.ClaimAsync("forbidden", Minute));

        Assert.Equal(PostgresErrorCodes.CheckViolation, thrown.SqlState);
    }
}
