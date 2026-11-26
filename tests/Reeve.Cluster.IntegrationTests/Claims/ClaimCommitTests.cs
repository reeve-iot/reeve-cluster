using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Claims;

public sealed class ClaimCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;

    public ClaimCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    private async Task<string> StallAsync(ClaimsRig rig)
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Db.ConnectionString).Database!;
        await rig.ExecAsync($"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
        return database;
    }

    [Fact]
    public async Task I33_a_grant_the_callers_token_cancelled_in_its_wait_for_the_standby_is_not_won_although_it_landed()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var database = await StallAsync(rig);

        using var cancel = new CancellationTokenSource();
        var claim = a.Claims.ClaimAsync("k", Minute, cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => claim.WaitAsync(Poll.Bound));
        Assert.NotNull(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task I33_a_grant_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_answers_Unknown_not_Won()
    {
        var time = new FakeTimeProvider();
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt, time);
        var a = await rig.AddReplicaAsync("a");
        var database = await StallAsync(rig);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await claim.WaitAsync(Poll.Bound);

        Assert.IsType<ClaimResult.Unknown>(result);
        Assert.NotNull(await rig.RowAsync("k"));
    }

    [Fact]
    public async Task I33_a_takeover_the_librarys_own_bound_cancelled_in_its_wait_for_the_standby_answers_Unknown_not_Won()
    {
        var time = new FakeTimeProvider();
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt, time);
        var holder = await rig.AddReplicaAsync("holder");
        var a = await rig.AddReplicaAsync("a");
        var first = await holder.WinAsync("k");
        await rig.EndLeaseAsync("holder");
        var database = await StallAsync(rig);

        var claim = a.Claims.ClaimAsync("k", Minute);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await claim.WaitAsync(Poll.Bound);

        Assert.IsType<ClaimResult.Unknown>(result);
        Assert.NotEqual(first, (await rig.RowAsync("k"))!.Token);
    }

    [Fact]
    public async Task I33_a_claim_fenced_commit_the_callers_token_cancelled_in_its_wait_for_the_standby_reports_an_unknown_outcome()
    {
        await using var rig = await ClaimsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.WinAsync("k");
        await using var connection = await rig.Db.OpenAsync();
        await Int.Owned.OwnedDatabase.ExecAsync(connection, "CREATE TABLE public.effect (id int PRIMARY KEY)");
        var database = await StallAsync(rig);

        await using var effect = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(rig.Db.ConnectionString) { Pooling = false, CommandTimeout = 0 }.ConnectionString);
        await effect.OpenAsync();
        await using var fenced = await a.Claims.BeginClaimFencedTransactionAsync("k", token, effect);
        await Int.Owned.OwnedDatabase.ExecAsync(effect, "INSERT INTO public.effect VALUES (1)");

        using var cancel = new CancellationTokenSource();
        var commit = fenced.CommitAsync(cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
        Assert.Equal(1L, await rig.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
    }
}
