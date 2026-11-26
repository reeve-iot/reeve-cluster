using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Primitives;

public sealed class SlotCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Ftt = TimeSpan.FromSeconds(5);

    private readonly PostgresFixture postgres;

    public SlotCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    private async Task<string> StallAsync(SlotsRig rig)
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Db.ConnectionString).Database!;
        await rig.Claims.ExecAsync($"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
        return database;
    }

    [Fact]
    public async Task I33_an_acquire_the_callers_token_cancelled_in_its_wait_for_the_standby_throws_although_the_slot_landed()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var database = await StallAsync(rig);

        using var cancel = new CancellationTokenSource();
        var acquire = a.Slots.AcquireAsync("k", "i", 1, TimeSpan.FromMinutes(1), cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acquire.WaitAsync(Poll.Bound));
        Assert.NotNull(await rig.RowAsync("k", "i"));
    }

    [Fact]
    public async Task I33_an_acquire_the_librarys_own_bound_cancelled_answers_Unknown_not_Acquired_and_the_process_holds_no_slot()
    {
        var time = new FakeTimeProvider();
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt, time);
        var a = await rig.AddReplicaAsync("a");
        var database = await StallAsync(rig);

        var acquire = a.AcquireAsync("k", "i");
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        var result = await acquire.WaitAsync(Poll.Bound);

        Assert.IsType<SlotAcquireResult.Unknown>(result);
        var row = await rig.RowAsync("k", "i");
        Assert.NotNull(row);
        Assert.Equal(SlotRenewResult.Ended, await a.RenewAsync("k", "i", row!.Token));
    }

    [Fact]
    public async Task I33_a_renewal_the_librarys_own_bound_cancelled_answers_Unknown()
    {
        var time = new FakeTimeProvider();
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt, time);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i", ttl: TimeSpan.FromHours(1));
        var database = await StallAsync(rig);

        var renewal = a.RenewAsync("k", "i", token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        Assert.Equal(SlotRenewResult.Unknown, await renewal.WaitAsync(Poll.Bound));
    }

    [Fact]
    public async Task I33_a_release_the_librarys_own_bound_cancelled_throws_CommitOutcomeUnknownException()
    {
        var time = new FakeTimeProvider();
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt, time);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        var database = await StallAsync(rig);

        var release = a.Slots.ReleaseAsync("k", "i", token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        time.Advance(Ftt + TimeSpan.FromSeconds(3));

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => release.WaitAsync(Poll.Bound));
    }

    [Fact]
    public async Task I33_a_slot_fenced_commit_the_callers_token_cancelled_in_its_wait_for_the_standby_reports_an_unknown_outcome()
    {
        await using var rig = await SlotsRig.CreateAsync(postgres, Ftt);
        var a = await rig.AddReplicaAsync("a");
        var token = await a.TakeAsync("k", "i");
        await using var setup = await rig.Db.OpenAsync();
        await Int.Owned.OwnedDatabase.ExecAsync(setup, "CREATE TABLE public.effect (id int PRIMARY KEY)");
        var database = await StallAsync(rig);

        await using var effect = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(rig.Db.ConnectionString) { Pooling = false, CommandTimeout = 0 }.ConnectionString);
        await effect.OpenAsync();
        await using var fenced = await a.Slots.BeginSlotFencedTransactionAsync("k", "i", token, effect);
        await Int.Owned.OwnedDatabase.ExecAsync(effect, "INSERT INTO public.effect VALUES (1)");

        using var cancel = new CancellationTokenSource();
        var commit = fenced.CommitAsync(cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
        Assert.Equal(1L, await rig.Claims.ScalarAsync<long>("SELECT count(*) FROM public.effect"));
    }
}
