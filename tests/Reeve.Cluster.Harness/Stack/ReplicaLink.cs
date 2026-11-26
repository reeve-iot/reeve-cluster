namespace Reeve.Cluster.Harness.Stack;

internal sealed class LinkHold(ProxyHold postgres, ProxyHold redis)
{
    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        List<Exception>? failures = null;
        foreach (var hold in new[] { postgres, redis })
        {
            try
            {
                await hold.ReleaseAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException(failures);
        }
    }
}

internal sealed class ReplicaLink : IAsyncDisposable
{
    internal ReplicaLink(string name, FaultProxy postgres, FaultProxy redis, string postgresConnectionString, string redisConnectionString)
    {
        Name = name;
        Postgres = postgres;
        Redis = redis;
        PostgresConnectionString = postgresConnectionString;
        RedisConnectionString = redisConnectionString;
    }

    public string Name { get; }

    public FaultProxy Postgres { get; }

    public FaultProxy Redis { get; }

    public string PostgresConnectionString { get; }

    public string RedisConnectionString { get; }

    public async Task CutAsync(CancellationToken cancellationToken = default)
    {
        await Postgres.CutAsync(cancellationToken).ConfigureAwait(false);
        await Redis.CutAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task RestoreAsync(CancellationToken cancellationToken = default)
    {
        await Postgres.RestoreAsync(cancellationToken).ConfigureAwait(false);
        await Redis.RestoreAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<LinkHold> HoldAsync(CancellationToken cancellationToken = default)
    {
        var pg = await Postgres.HoldAsync(null, cancellationToken).ConfigureAwait(false);
        try
        {
            var rd = await Redis.HoldAsync(null, cancellationToken).ConfigureAwait(false);
            return new LinkHold(pg, rd);
        }
        catch
        {
            await pg.ReleaseAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await Postgres.ClearAsync(cancellationToken).ConfigureAwait(false);
        await Redis.ClearAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => DisposeAsync(releasePorts: true);

    public async ValueTask DisposeAsync(bool releasePorts)
    {
        List<Exception>? failures = null;
        foreach (var proxy in new[] { Postgres, Redis })
        {
            try
            {
                await proxy.DisposeAsync(releasePorts).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException(failures);
        }
    }
}
