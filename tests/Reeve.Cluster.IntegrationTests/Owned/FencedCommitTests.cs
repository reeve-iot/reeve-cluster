using Microsoft.EntityFrameworkCore;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class FencedCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;

    public FencedCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => SyncStandby.SetAsync(postgres, string.Empty);

    [Fact]
    public async Task I33_a_commit_the_Npgsql_helper_cancelled_in_its_wait_for_the_standby_reports_an_unknown_outcome_although_it_landed()
    {
        var (db, token, database) = await StalledAsync();
        await using var connection = await db.OpenAsync();
        await using var fenced = await new FencedOwner("unit", "k", token, db.Timeline).BeginFencedTransactionAsync(connection);
        await ExecAsync(connection, "INSERT INTO public.effect (tenure) VALUES (@tenure)", ("tenure", token));

        using var cancel = new CancellationTokenSource();
        var commit = fenced.CommitAsync(cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
        Assert.Equal(1L, await CountAsync(db));
    }

    [Fact]
    public async Task I33_a_commit_the_EF_helper_cancelled_in_its_wait_for_the_standby_reports_an_unknown_outcome_although_it_landed()
    {
        var (db, token, database) = await StalledAsync();
        await using var context = EffectDb.Over(db.ConnectionString);
        await using var fenced = await new FencedOwner("unit", "k", token, db.Timeline).BeginFencedTransactionAsync(context);
        context.Effects.Add(new Effect { Tenure = token });
        await context.SaveChangesAsync();

        using var cancel = new CancellationTokenSource();
        var commit = fenced.CommitAsync(cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
        Assert.Equal(1L, await CountAsync(db));
    }

    [Fact]
    public async Task I33_a_commit_whose_wait_the_connections_Command_Timeout_ended_reports_an_unknown_outcome()
    {
        var (db, token, _) = await StalledAsync();
        var connectionString = new NpgsqlConnectionStringBuilder(db.ConnectionString) { CommandTimeout = 1 }.ConnectionString;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var fenced = await new FencedOwner("unit", "k", token, db.Timeline).BeginFencedTransactionAsync(connection);
        await ExecAsync(connection, "INSERT INTO public.effect (tenure) VALUES (@tenure)", ("tenure", token));

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => fenced.CommitAsync().WaitAsync(Poll.Bound));
        Assert.Equal(1L, await CountAsync(db));
    }

    [Fact]
    public async Task I33_a_commit_whose_backend_is_terminated_in_its_wait_for_the_standby_reports_an_unknown_outcome()
    {
        var (db, token, database) = await StalledAsync();

        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(db.ConnectionString) { CommandTimeout = 0 }.ConnectionString);
        await connection.OpenAsync();
        await using var fenced = await new FencedOwner("unit", "k", token, db.Timeline).BeginFencedTransactionAsync(connection);
        await ExecAsync(connection, "INSERT INTO public.effect (tenure) VALUES (@tenure)", ("tenure", token));

        var commit = fenced.CommitAsync();
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await using (var admin = await postgres.OpenConnectionAsync())
        {
            Assert.True(await ScalarAsync<bool>(admin, "SELECT pg_terminate_backend(@pid)", ("pid", connection.ProcessID)));
        }

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
        Assert.Equal(1L, await CountAsync(db));
    }

    [Fact]
    public async Task I33_the_EF_contexts_own_CommandTimeout_does_not_end_the_commits_wait_for_the_standby()
    {
        var (db, token, database) = await StalledAsync();

        var connectionString = new NpgsqlConnectionStringBuilder(db.ConnectionString) { CommandTimeout = 0 }.ConnectionString;
        await using var context = new EffectDb(new DbContextOptionsBuilder<EffectDb>().UseNpgsql(connectionString, o => o.CommandTimeout(1)).Options);
        await using var fenced = await new FencedOwner("unit", "k", token, db.Timeline).BeginFencedTransactionAsync(context);
        context.Effects.Add(new Effect { Tenure = token });
        await context.SaveChangesAsync();

        using var cancel = new CancellationTokenSource();
        var commit = fenced.CommitAsync(cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        Assert.False(commit.IsCompleted, "EF's CommandTimeout(1) ended the commit's wait for the standby");
        await cancel.CancelAsync();

        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => commit.WaitAsync(Poll.Bound));
    }

    private async Task<(OwnedDatabase Db, long Token, string Database)> StalledAsync()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        var database = new NpgsqlConnectionStringBuilder(db.ConnectionString).Database!;
        await using (var connection = await db.OpenAsync())
        {
            await EffectDb.CreateTableAsync(connection);
            await ExecAsync(connection, $"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        }

        await SyncStandby.StallAsync(postgres);
        return (db, token, database);
    }

    private static async Task<long> CountAsync(OwnedDatabase db)
    {
        await using var context = EffectDb.Over(db.ConnectionString);
        return await context.Effects.LongCountAsync();
    }
}
