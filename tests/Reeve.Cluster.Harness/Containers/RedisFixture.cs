using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

namespace Reeve.Cluster.Harness.Containers;

public sealed class RedisFixture : IAsyncLifetime
{
    public const string Image = "redis:8.10.2-alpine";

    private readonly RedisContainer _container = new RedisBuilder(Image).Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    public Task<ConnectionMultiplexer> ConnectAsync() => ConnectionMultiplexer.ConnectAsync(ConnectionString);
}
