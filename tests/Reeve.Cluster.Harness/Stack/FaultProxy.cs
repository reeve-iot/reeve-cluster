namespace Reeve.Cluster.Harness.Stack;

internal enum Direction
{
    Upstream,

    Downstream,
}

internal sealed class ProxyToxic
{
    private readonly FaultProxy proxy;
    private int removed;

    internal ProxyToxic(FaultProxy proxy, string name)
    {
        this.proxy = proxy;
        Name = name;
    }

    public string Name { get; }

    public async Task RemoveAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref removed, 1) != 0)
        {
            return;
        }

        try
        {
            await proxy.RemoveToxicAsync(Name, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Interlocked.Exchange(ref removed, 0);
            throw;
        }
    }
}

internal sealed class ProxyHold
{
    private readonly FaultProxy proxy;
    private readonly IReadOnlyList<Direction> directions;
    private readonly int epoch;
    private int released;

    internal ProxyHold(FaultProxy proxy, IReadOnlyList<Direction> directions, int epoch)
    {
        this.proxy = proxy;
        this.directions = directions;
        this.epoch = epoch;
    }

    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref released, 1) != 0)
        {
            return;
        }

        try
        {
            await proxy.ReleaseHoldAsync(directions, epoch, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            Interlocked.Exchange(ref released, 0);
            throw;
        }
    }
}

internal sealed class FaultProxy : IAsyncDisposable
{
    private static readonly int HoldMilliseconds = (int)TimeSpan.FromDays(1).TotalMilliseconds;

    private readonly ToxiproxyClient client;
    private readonly Action<string> release;
    private readonly HashSet<string> toxics = [];
    private readonly Dictionary<Direction, int> holds = [];
    private readonly SemaphoreSlim holdGate = new(1, 1);
    private readonly object gate = new();
    private int holdEpoch;
    private int counter;
    private bool disposed;

    internal FaultProxy(ToxiproxyClient client, string name, string host, int port, string upstream, Action<string> release)
    {
        this.client = client;
        Name = name;
        Host = host;
        Port = port;
        Upstream = upstream;
        this.release = release;
    }

    public string Name { get; }

    public string Host { get; }

    public int Port { get; }

    public string Upstream { get; private set; }

    public bool IsCut { get; private set; }

    public bool IsDisposed
    {
        get
        {
            lock (gate)
            {
                return disposed;
            }
        }
    }

    public async Task CutAsync(CancellationToken cancellationToken = default)
    {
        await client.SetEnabledAsync(Name, false, cancellationToken).ConfigureAwait(false);
        IsCut = true;
    }

    public async Task RepointAsync(string upstream, CancellationToken cancellationToken = default)
    {
        await client.SetUpstreamAsync(Name, upstream, !IsCut, cancellationToken).ConfigureAwait(false);
        Upstream = upstream;
    }

    public async Task RestoreAsync(CancellationToken cancellationToken = default)
    {
        await client.SetEnabledAsync(Name, true, cancellationToken).ConfigureAwait(false);
        IsCut = false;
    }

    public async Task<ProxyHold> HoldAsync(Direction? direction = null, CancellationToken cancellationToken = default)
    {
        var directions = direction is { } one ? [one] : Enum.GetValues<Direction>();
        await holdGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var taken = new List<Direction>();
            try
            {
                foreach (var d in directions)
                {
                    if (holds.GetValueOrDefault(d) == 0)
                    {
                        await client
                            .AddToxicAsync(Name, HoldToxic(d), "latency", d == Direction.Upstream ? "upstream" : "downstream", new Dictionary<string, object> { ["latency"] = HoldMilliseconds, ["jitter"] = 0 }, cancellationToken)
                            .ConfigureAwait(false);
                    }

                    holds[d] = holds.GetValueOrDefault(d) + 1;
                    taken.Add(d);
                }
            }
            catch
            {
                foreach (var d in taken)
                {
                    await ReleaseLockedAsync(d, CancellationToken.None).ConfigureAwait(false);
                }

                throw;
            }

            return new ProxyHold(this, directions, holdEpoch);
        }
        finally
        {
            holdGate.Release();
        }
    }

    internal async Task ReleaseHoldAsync(IReadOnlyList<Direction> directions, int epoch, CancellationToken cancellationToken)
    {
        await holdGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (epoch != holdEpoch)
            {
                return;
            }

            foreach (var d in directions)
            {
                await ReleaseLockedAsync(d, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            holdGate.Release();
        }
    }

    private static string HoldToxic(Direction d) => $"hold-{d}";

    private async Task ReleaseLockedAsync(Direction d, CancellationToken cancellationToken)
    {
        var left = holds.GetValueOrDefault(d) - 1;
        holds[d] = Math.Max(left, 0);
        if (left == 0)
        {
            await client.RemoveToxicAsync(Name, HoldToxic(d), cancellationToken).ConfigureAwait(false);
        }
    }

    public Task<ProxyToxic> LatencyAsync(TimeSpan latency, TimeSpan jitter, Direction direction, CancellationToken cancellationToken = default) =>
        AddAsync(
            "latency",
            "latency",
            direction,
            new Dictionary<string, object> { ["latency"] = (int)latency.TotalMilliseconds, ["jitter"] = (int)jitter.TotalMilliseconds },
            cancellationToken);

    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await holdGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            foreach (var d in Enum.GetValues<Direction>())
            {
                if (holds.GetValueOrDefault(d) > 0)
                {
                    await client.RemoveToxicAsync(Name, HoldToxic(d), cancellationToken).ConfigureAwait(false);
                }

                holds[d] = 0;
            }

            holdEpoch++;
        }
        finally
        {
            holdGate.Release();
        }

        string[] names;
        lock (gate)
        {
            names = [.. toxics];
        }

        foreach (var name in names)
        {
            await RemoveToxicAsync(name, cancellationToken).ConfigureAwait(false);
        }

        if (IsCut)
        {
            await RestoreAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => DisposeAsync(releasePort: true);

    public async ValueTask DisposeAsync(bool releasePort)
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
        }

        try
        {
            await client.DeleteProxyAsync(Name).ConfigureAwait(false);
        }
        finally
        {
            if (releasePort)
            {
                release(Name);
            }
        }
    }

    internal async Task RemoveToxicAsync(string toxic, CancellationToken cancellationToken)
    {
        bool had;
        lock (gate)
        {
            had = toxics.Remove(toxic);
        }

        if (had)
        {
            await client.RemoveToxicAsync(Name, toxic, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<ProxyToxic> AddAsync(string kind, string type, Direction direction, Dictionary<string, object> attributes, CancellationToken cancellationToken)
    {
        string name;
        lock (gate)
        {
            name = $"{kind}-{direction}-{++counter}";
            toxics.Add(name);
        }

        try
        {
            await client
                .AddToxicAsync(Name, name, type, direction == Direction.Upstream ? "upstream" : "downstream", attributes, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            lock (gate)
            {
                toxics.Remove(name);
            }

            throw;
        }

        return new ProxyToxic(this, name);
    }
}
