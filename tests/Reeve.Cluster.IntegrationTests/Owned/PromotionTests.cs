using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Testcontainers.PostgreSql;
using static Int.Owned.OwnedDatabase;

namespace Int.Owned;

public sealed class PromotionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder(PostgresFixture.Image).Build();

    public Task InitializeAsync() => container.StartAsync();

    public Task DisposeAsync() => container.DisposeAsync().AsTask();

    private string ConnectionString => new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Pooling = false }.ConnectionString;

    private async Task PromoteAsync()
    {
        var signal = await container.ExecAsync(["bash", "-c", "touch \"$PGDATA/standby.signal\""]);
        Assert.True(signal.ExitCode == 0, signal.Stderr);
        await container.StopAsync();
        await container.StartAsync();

        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        Assert.True(await ScalarAsync<bool>(connection, "SELECT pg_is_in_recovery()"), "The restarted server is not a standby.");
        Assert.True(await ScalarAsync<bool>(connection, "SELECT pg_promote(wait => true)"));
    }

    [Fact]
    public async Task I10_I32_after_a_promotion_a_draw_repeats_no_token_of_the_lost_suffix_and_a_fenced_write_of_the_old_timeline_raises_RC002()
    {
        var db = await OwnedDatabase.MigrateAsync(ConnectionString);
        Assert.Equal(1L, db.Timeline);
        await db.AddMemberAsync("a", lease: "10 minutes");
        var token = await db.CreateOwnedAsync("unit", "k", "a");
        var drawn = new List<long> { token };
        await using (var connection = await db.OpenAsync())
        {
            for (var i = 0; i < 3; i++)
            {
                drawn.Add(await ScalarAsync<long>(connection, "SELECT reeve_cluster.draw_token()"));
            }
        }

        await PromoteAsync();

        db = await OwnedDatabase.MigrateAsync(ConnectionString);
        Assert.Equal(2L, db.Timeline);
        await using var after = await db.OpenAsync();
        Assert.Equal(2L, await ScalarAsync<long>(after, "SELECT reeve_cluster.server_timeline()"));

        await db.LoseSuffixAsync(sequenceBack: SequenceValueOf(drawn[0]));

        Assert.Equal("RC002", await SqlStateOfAsync(() => ExecAsync(after, "SELECT reeve_cluster.assert_owner('unit', 'k', @t, 1)", ("t", token))));
        Assert.Equal("RC002", await SqlStateOfAsync(() => ExecAsync(after, "SELECT reeve_cluster.owner_is('unit', 'k', @t, 1)", ("t", token))));
        await ExecAsync(after, "SELECT reeve_cluster.assert_owner('unit', 'k', @t, 2)", ("t", token));

        var first = await ScalarAsync<long>(after, "SELECT reeve_cluster.draw_token()");
        var second = await ScalarAsync<long>(after, "SELECT reeve_cluster.draw_token()");

        Assert.Equal(SequenceValueOf(drawn[1]), SequenceValueOf(first));
        Assert.All(new[] { first, second }, t => Assert.Equal(2L, TimelineOf(t)));
        Assert.True(first > drawn.Max(), $"The first draw on timeline 2 returned {first}; timeline 1 handed out {string.Join(", ", drawn)}.");
        Assert.True(second > first, $"{second} > {first}");
        Assert.DoesNotContain(first, drawn);
    }
}
