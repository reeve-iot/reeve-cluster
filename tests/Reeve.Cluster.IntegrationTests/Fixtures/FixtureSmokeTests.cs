using Reeve.Cluster.Harness.Containers;

namespace Int.Fixtures;

/// <summary>The containers start, and they are the versions the rest of the suite assumes.</summary>
public sealed class FixtureSmokeTests : IClassFixture<PostgresFixture>, IClassFixture<RedisFixture>
{
    private readonly PostgresFixture _postgres;
    private readonly RedisFixture _redis;

    public FixtureSmokeTests(PostgresFixture postgres, RedisFixture redis)
    {
        _postgres = postgres;
        _redis = redis;
    }

    [Fact]
    public async Task Postgres_fixture_runs_PostgreSQL_18()
    {
        await using var connection = await _postgres.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "select current_setting('server_version_num')::int";

        var version = (int)(await command.ExecuteScalarAsync())!;

        Assert.InRange(version, 180000, 189999);
    }

    [Fact]
    public async Task Redis_fixture_answers_a_ping()
    {
        await using var redis = await _redis.ConnectAsync();

        var latency = await redis.GetDatabase().PingAsync();

        Assert.True(latency >= TimeSpan.Zero);
    }
}
