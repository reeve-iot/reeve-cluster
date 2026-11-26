using System.Data;
using System.Diagnostics;
using Int.Capture;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class FencedTransactionTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public FencedTransactionTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public enum Helper
    {
        Npgsql,
        Ef,
    }

    public enum Scope
    {
        Session,
        Role,
        Database,
    }

    [Theory]
    [InlineData(Helper.Npgsql)]
    [InlineData(Helper.Ef)]
    public async Task I11_a_fenced_transaction_runs_under_FencedTransactionTimeout_and_holds_the_row_FOR_SHARE_until_its_commit(Helper helper)
    {
        var (db, token) = await OwnedAsync();
        var owner = new FencedOwner("unit", "k", token, db.Timeline, TimeSpan.FromMinutes(5));

        await using (var fenced = await BeginAsync(helper, owner, db.ConnectionString))
        {
            Assert.Equal("5min", await fenced.ScalarAsync<string>("SELECT current_setting('transaction_timeout')"));
            Assert.Equal([OwnedFaultPoints.FencedAfterAssert.Name], owner.Reached);
            Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
            Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR UPDATE"));

            await fenced.WriteAsync(token);
            await fenced.CommitAsync();
        }

        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
        Assert.Equal([token], await EffectsAsync(db));
    }

    [Theory]
    [InlineData(Helper.Npgsql)]
    [InlineData(Helper.Ef)]
    public async Task Tokens_a_delayed_operation_across_a_freeing_a_recreation_and_a_new_ownership_is_refused(Helper helper)
    {
        var (db, stale) = await OwnedAsync();
        await using (var connection = await db.OpenAsync())
        {
            await ExecAsync(connection, "UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = 'unit' AND key = 'k'");
            await ExecAsync(connection, "DELETE FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k'");
        }

        var current = await db.CreateOwnedAsync("unit", "k", "a");

        var refused = await Assert.ThrowsAsync<OwnershipLostException>(() => BeginAsync(helper, new FencedOwner("unit", "k", stale, db.Timeline), db.ConnectionString));
        Assert.Equal("RC001", Assert.IsType<PostgresException>(refused.InnerException).SqlState);
        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));

        await using var fenced = await BeginAsync(helper, new FencedOwner("unit", "k", current, db.Timeline), db.ConnectionString);
        await fenced.WriteAsync(current);
        await fenced.CommitAsync();
        Assert.Equal([current], await EffectsAsync(db));
    }

    [Theory]
    [InlineData(Helper.Npgsql)]
    [InlineData(Helper.Ef)]
    public async Task I12_a_fenced_transaction_held_open_across_an_adoption_commits_before_it_and_the_old_tenure_is_refused_after(Helper helper)
    {
        var (db, first) = await OwnedAsync();
        await db.AddMemberAsync("b");
        var owner = new FencedOwner("unit", "k", first, db.Timeline);

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        Task<long?> adopt;
        await using (var fenced = await BeginAsync(helper, owner, db.ConnectionString))
        {
            await fenced.WriteAsync(first);
            adopt = AdoptAsync(adopter, "unit", "k", first, "b");
            await db.WaitUntilBlockedAsync(adopter.ProcessID, Poll.Bound);
            Assert.False(adopt.IsCompleted);

            await fenced.CommitAsync();
        }

        var second = await adopt.WaitAsync(Poll.Bound);
        await adoption.CommitAsync();
        Assert.True(second > first);

        await Assert.ThrowsAsync<OwnershipLostException>(() => BeginAsync(helper, owner, db.ConnectionString));
        Assert.Equal([first], await EffectsAsync(db));
    }

    [Fact]
    public async Task I11_a_fenced_transaction_that_begins_while_an_adoption_holds_the_row_waits_for_it_and_is_refused()
    {
        var (db, first) = await OwnedAsync();
        await db.AddMemberAsync("b");

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        Assert.NotNull(await AdoptAsync(adopter, "unit", "k", first, "b"));

        await using var connection = await db.OpenAsync();
        var begin = new FencedOwner("unit", "k", first, db.Timeline).BeginFencedTransactionAsync(connection);
        await db.WaitUntilBlockedAsync(connection.ProcessID, Poll.Bound);
        await adoption.CommitAsync();

        var refused = await Assert.ThrowsAsync<OwnershipLostException>(() => begin.WaitAsync(Poll.Bound));
        Assert.Equal("RC001", Assert.IsType<PostgresException>(refused.InnerException).SqlState);
    }

    [Theory]
    [InlineData(IsolationLevel.RepeatableRead)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task F7_under_the_applications_higher_isolation_a_wait_for_a_change_of_owner_stays_a_serialization_failure_and_the_retry_is_refused(IsolationLevel level)
    {
        var (db, first) = await OwnedAsync();
        await db.AddMemberAsync("b");
        var owner = new FencedOwner("unit", "k", first, db.Timeline);

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        Assert.NotNull(await AdoptAsync(adopter, "unit", "k", first, "b"));

        await using var connection = await db.OpenAsync();
        var begin = owner.BeginFencedTransactionAsync(connection, level);
        await db.WaitUntilBlockedAsync(connection.ProcessID, Poll.Bound);
        await adoption.CommitAsync();

        var failure = await Assert.ThrowsAsync<PostgresException>(() => begin.WaitAsync(Poll.Bound));
        Assert.Equal(PostgresErrorCodes.SerializationFailure, failure.SqlState);

        await Assert.ThrowsAsync<OwnershipLostException>(() => owner.BeginFencedTransactionAsync(connection, level));
    }

    [Fact]
    public async Task Ownership_FencedTransactionTimeout_ends_a_fenced_transaction_held_open_and_the_waiting_adoption_proceeds()
    {
        var (db, first) = await OwnedAsync();
        await db.AddMemberAsync("b");
        var timeout = TimeSpan.FromSeconds(1);

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        await using var connection = await db.OpenAsync();
        var started = Stopwatch.GetTimestamp();
        await using var fenced = await new FencedOwner("unit", "k", first, db.Timeline, timeout).BeginFencedTransactionAsync(connection);
        var adopt = AdoptAsync(adopter, "unit", "k", first, "b");

        Assert.NotNull(await adopt.WaitAsync(Poll.Bound));
        Assert.True(Stopwatch.GetElapsedTime(started) >= timeout, "the adoption waited for the fenced transaction's bound");
        await adoption.CommitAsync();

        var ended = await Assert.ThrowsAsync<PostgresException>(() => fenced.CommitAsync());
        Assert.Equal("25P04", ended.SqlState);
    }

    [Theory]
    [InlineData(Helper.Npgsql, Scope.Session)]
    [InlineData(Helper.Npgsql, Scope.Role)]
    [InlineData(Helper.Npgsql, Scope.Database)]
    [InlineData(Helper.Ef, Scope.Session)]
    [InlineData(Helper.Ef, Scope.Role)]
    [InlineData(Helper.Ef, Scope.Database)]
    public async Task A_connection_whose_session_role_or_database_sets_transaction_timeout_is_refused(Helper helper, Scope scope)
    {
        var (db, token) = await OwnedAsync();
        var connectionString = await SetTransactionTimeoutAsync(db, scope);
        var owner = new FencedOwner("unit", "k", token, db.Timeline);

        var refused = await Assert.ThrowsAsync<ClusterConfigurationException>(() => BeginAsync(helper, owner, connectionString, session: scope == Scope.Session));

        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, refused.Reason);
        Assert.Equal(1, owner.Starts);
        Assert.Empty(owner.Reached);
        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
    }

    [Theory]
    [InlineData(Helper.Npgsql)]
    [InlineData(Helper.Ef)]
    public async Task I32_a_fenced_transaction_on_another_timeline_throws_OwnershipLostException_and_raises_the_timeline_trigger(Helper helper)
    {
        var (db, token) = await OwnedAsync();
        var owner = new FencedOwner("unit", "k", token, db.LaterTimeline);

        var refused = await Assert.ThrowsAsync<OwnershipLostException>(() => BeginAsync(helper, owner, db.ConnectionString));

        Assert.Equal("RC002", Assert.IsType<PostgresException>(refused.InnerException).SqlState);
        Assert.Equal(1, owner.OtherTimelines);
        Assert.True(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
    }

    [Theory]
    [InlineData(Helper.Npgsql)]
    [InlineData(Helper.Ef)]
    public async Task EnsureOwner_runs_first_so_a_stopped_tenure_sends_nothing(Helper helper)
    {
        var (db, token) = await OwnedAsync();
        var owner = new FencedOwner("unit", "k", token, db.Timeline) { Refusal = new OwnershipLostException("stopping") };

        var unreachable = new NpgsqlConnectionStringBuilder(db.ConnectionString) { Database = "reeve_absent" }.ConnectionString;
        var refused = await Assert.ThrowsAsync<OwnershipLostException>(() => BeginAsync(helper, owner, unreachable, open: false));
        Assert.Equal("stopping", refused.Message);
    }

    [Fact]
    public async Task I4_the_EF_helpers_transaction_takes_the_owned_share_lock_first_and_the_captured_entries_last()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        var db = await MigrateAsync(rig.Log.ConnectionString);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        var owner = new FencedOwner("unit", "k", token, db.Timeline);

        await using var context = rig.NewContext();
        await using (var fenced = await owner.BeginFencedTransactionAsync(context))
        {
            context.Settings.Add(new Setting { Id = 1, Name = "n", Value = "v" });
            await context.SaveChangesAsync();

            var pid = ((NpgsqlConnection)context.Database.GetDbConnection()).ProcessID;
            Assert.False(await db.CanLockOwnedAsync("unit", "k", "FOR NO KEY UPDATE"));
            await using (var connection = await db.OpenAsync())
            {
                Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM pg_locks WHERE pid = @pid AND locktype = 'advisory'", ("pid", pid)));
            }

            await fenced.CommitAsync();
        }

        Assert.Equal([("setting", (string?)"n")], await rig.EntriesAsync());
    }

    [Fact]
    public async Task Ownership_the_bound_is_armed_before_assert_owner_waits_for_the_row_so_FencedTransactionTimeout_ends_the_wait()
    {
        var (db, first) = await OwnedAsync();

        await using var adopter = await db.OpenAsync();
        await using var adoption = await adopter.BeginTransactionAsync();
        await ExecAsync(adopter, "SELECT 1 FROM reeve_cluster.owned WHERE kind = 'unit' AND key = 'k' FOR NO KEY UPDATE");

        await using var connection = await db.OpenAsync();
        var begin = new FencedOwner("unit", "k", first, db.Timeline, TimeSpan.FromSeconds(1)).BeginFencedTransactionAsync(connection);

        var ended = await Assert.ThrowsAsync<PostgresException>(() => begin.WaitAsync(Poll.Bound));
        Assert.Equal("25P04", ended.SqlState);
    }

    [Fact]
    public async Task A_connection_object_reopened_on_another_backend_is_checked_again()
    {
        var (db, token) = await OwnedAsync();
        var owner = new FencedOwner("unit", "k", token, db.Timeline);

        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(db.ConnectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync();
        var first = connection.ProcessID;
        await using (var fenced = await owner.BeginFencedTransactionAsync(connection))
        {
            await fenced.CommitAsync();
        }

        await connection.CloseAsync();
        await SetTransactionTimeoutAsync(db, Scope.Database);
        await connection.OpenAsync();
        Assert.NotEqual(first, connection.ProcessID);

        var refused = await Assert.ThrowsAsync<ClusterConfigurationException>(() => owner.BeginFencedTransactionAsync(connection));
        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, refused.Reason);
    }

    [Fact]
    public async Task A_refused_connection_is_refused_again_on_the_next_begin()
    {
        var (db, token) = await OwnedAsync();
        var owner = new FencedOwner("unit", "k", token, db.Timeline);
        await using var connection = await db.OpenAsync();
        await ExecAsync(connection, "SET transaction_timeout = '60s'");

        await Assert.ThrowsAsync<ClusterConfigurationException>(() => owner.BeginFencedTransactionAsync(connection));
        var again = await Assert.ThrowsAsync<ClusterConfigurationException>(() => owner.BeginFencedTransactionAsync(connection));
        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, again.Reason);
    }

    private async Task<(OwnedDatabase Db, long Token)> OwnedAsync()
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        await using var connection = await db.OpenAsync();
        await EffectDb.CreateTableAsync(connection);
        return (db, token);
    }

    private static async Task<List<long>> EffectsAsync(OwnedDatabase db)
    {
        await using var context = EffectDb.Over(db.ConnectionString);
        return await context.Effects.OrderBy(e => e.Id).Select(e => e.Tenure).ToListAsync();
    }

    private async Task<string> SetTransactionTimeoutAsync(OwnedDatabase db, Scope scope)
    {
        var builder = new NpgsqlConnectionStringBuilder(db.ConnectionString);
        await using var admin = await postgres.OpenConnectionAsync();
        switch (scope)
        {
            case Scope.Database:
                await ExecAsync(admin, $"ALTER DATABASE \"{builder.Database}\" SET transaction_timeout = '60s'");
                break;
            case Scope.Role:
                var role = "fenced_" + Guid.NewGuid().ToString("N")[..12];
                await ExecAsync(admin, $"CREATE ROLE {role} LOGIN PASSWORD 'fenced'");
                await ExecAsync(admin, $"ALTER ROLE {role} SET transaction_timeout = '60s'");
                builder.Username = role;
                builder.Password = "fenced";
                break;
            case Scope.Session:
                break;
        }

        return builder.ConnectionString;
    }

    private static async Task<Fenced> BeginAsync(Helper helper, FencedOwner owner, string connectionString, IsolationLevel level = IsolationLevel.Unspecified, bool session = false, bool open = true)
    {
        if (helper == Helper.Npgsql)
        {
            var connection = new NpgsqlConnection(connectionString);
            try
            {
                if (open)
                {
                    await connection.OpenAsync();
                }

                if (session)
                {
                    await ExecAsync(connection, "SET transaction_timeout = '60s'");
                }

                var transaction = await owner.BeginFencedTransactionAsync(connection, level);
                return new Fenced(transaction.CommitAsync, transaction, connection, connection, null);
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }
        }

        var context = EffectDb.Over(connectionString);
        try
        {
            if (session)
            {
                await context.Database.OpenConnectionAsync();
                await context.Database.ExecuteSqlRawAsync("SET transaction_timeout = '60s'");
            }

            var transaction = await owner.BeginFencedTransactionAsync(context, level);
            return new Fenced(transaction.CommitAsync, transaction, context, (NpgsqlConnection)context.Database.GetDbConnection(), context);
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }

    private sealed class Fenced(
        Func<CancellationToken, Task> commit, IAsyncDisposable transaction, IAsyncDisposable resource, NpgsqlConnection connection, EffectDb? context) : IAsyncDisposable
    {
        public Task CommitAsync() => commit(CancellationToken.None);

        public Task<T> ScalarAsync<T>(string sql) => OwnedDatabase.ScalarAsync<T>(connection, sql);

        public async Task WriteAsync(long tenure)
        {
            if (context is null)
            {
                await ExecAsync(connection, "INSERT INTO public.effect (tenure) VALUES (@tenure)", ("tenure", tenure));
                return;
            }

            context.Effects.Add(new Effect { Tenure = tenure });
            await context.SaveChangesAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await transaction.DisposeAsync();
            await resource.DisposeAsync();
        }
    }
}
