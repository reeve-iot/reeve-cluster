using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Testcontainers.PostgreSql;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class TwoPromotionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder(PostgresFixture.Image).Build();

    public Task InitializeAsync() => container.StartAsync();

    public Task DisposeAsync() => container.DisposeAsync().AsTask();

    private string ConnectionString => new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Pooling = false }.ConnectionString;

    private async Task RestartAsStandbyAsync()
    {
        var signal = await container.ExecAsync(["bash", "-c", "touch \"$PGDATA/standby.signal\""]);
        Assert.True(signal.ExitCode == 0, signal.Stderr);
        await container.StopAsync();
        await container.StartAsync();

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        Assert.True(await ScalarAsync<bool>(connection, "SELECT pg_is_in_recovery()"), "The restarted server is not a standby.");
    }

    private async Task PromoteAsync()
    {
        await RestartAsStandbyAsync();
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        Assert.True(await ScalarAsync<bool>(connection, "SELECT pg_promote(wait => true)"));
    }

    private static async Task<List<long>> DrawAsync(OwnedDatabase db, int count)
    {
        await using var connection = await db.OpenAsync();
        var tokens = new List<long>();
        for (var i = 0; i < count; i++)
        {
            tokens.Add(await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()"));
        }

        return tokens;
    }

    [Fact]
    public async Task I10_I32_after_two_lost_suffixes_no_token_repeats_and_each_timelines_tokens_are_above_the_one_before()
    {
        var db = await OwnedDatabase.MigrateAsync(ConnectionString);
        Assert.Equal(1L, db.Timeline);
        var onOne = await DrawAsync(db, 5);

        await PromoteAsync();
        db = await OwnedDatabase.MigrateAsync(ConnectionString);
        Assert.Equal(2L, db.Timeline);
        await db.LoseSuffixAsync(sequenceBack: SequenceValueOf(onOne[0]));
        var onTwo = await DrawAsync(db, 5);

        await PromoteAsync();
        db = await OwnedDatabase.MigrateAsync(ConnectionString);
        Assert.Equal(3L, db.Timeline);
        await db.LoseSuffixAsync(sequenceBack: SequenceValueOf(onOne[0]));
        var onThree = await DrawAsync(db, 5);

        Assert.Equal(SequenceValueOf(onOne[1]), SequenceValueOf(onTwo[0]));
        Assert.Equal(SequenceValueOf(onOne[1]), SequenceValueOf(onThree[0]));
        Assert.True(onTwo.Min() > onOne.Max(), $"Timeline 2 drew {string.Join(", ", onTwo)}; timeline 1 had handed out {string.Join(", ", onOne)}.");
        Assert.True(onThree.Min() > onTwo.Max(), $"Timeline 3 drew {string.Join(", ", onThree)}; timeline 2 had handed out {string.Join(", ", onTwo)}.");
        Assert.Equal(15, onOne.Concat(onTwo).Concat(onThree).Distinct().Count());
    }

    [Fact]
    public async Task I24_on_a_standby_the_ownership_functions_fail_with_55000_not_RC002()
    {
        var db = await OwnedDatabase.MigrateAsync(ConnectionString);
        await db.AddMemberAsync("a", lease: "10 minutes");
        var token = await db.CreateOwnedAsync("unit", "k", "a");

        await RestartAsStandbyAsync();
        await using var c = new NpgsqlConnection(ConnectionString);
        await c.OpenAsync();

        Assert.Equal(PostgresErrorCodes.ObjectNotInPrerequisiteState, await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.server_timeline()")));
        Assert.Equal(PostgresErrorCodes.ObjectNotInPrerequisiteState, await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.draw_token()")));
        Assert.Equal(PostgresErrorCodes.ObjectNotInPrerequisiteState, await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.assert_owner('unit', 'k', @t, 1)", ("t", token))));
        Assert.Equal(PostgresErrorCodes.ObjectNotInPrerequisiteState, await SqlStateOfAsync(() => ExecAsync(c, "SELECT reeve_cluster.owner_is('unit', 'k', @t, 1)", ("t", token))));
    }
}
