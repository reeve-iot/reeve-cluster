using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Membership;

internal interface ICapabilityStore
{
    Task<bool> WriteAsync(string replicaId, long epoch, IReadOnlyList<string> capabilities, CancellationToken cancellationToken);
}

internal sealed class CapabilityStore(MemberWriteDataSource writes) : ICapabilityStore
{
    internal const string WriteSql = """
        UPDATE reeve_cluster.member SET capabilities = @capabilities
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()
        """;

    public async Task<bool> WriteAsync(string replicaId, long epoch, IReadOnlyList<string> capabilities, CancellationToken cancellationToken)
    {
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(WriteSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        command.Parameters.Add(new NpgsqlParameter("capabilities", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = capabilities.ToArray() });
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return rows == 1;
    }
}

internal sealed class ClusterCapabilitiesRegistry(ICapabilityStore store, LocalPhaseGate gate, IOptions<ClusterOptions> options, TimeProvider time)
    : IClusterCapabilities
{
    private readonly SemaphoreSlim changes = new(1, 1);
    private readonly HashSet<string> application = new(StringComparer.Ordinal);
    private readonly HashSet<string> units = new(StringComparer.Ordinal);
    private volatile IReadOnlyList<string> current = [];

    public IReadOnlyList<string> Current => current;

    internal void AddUnitKind(string kind)
    {
        var capability = ClusterCapabilities.ForUnit(kind);
        lock (units)
        {
            units.Add(capability);
            current = Snapshot();
        }
    }

    public async Task SetAsync(IEnumerable<string> capabilities, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        var next = capabilities.ToHashSet(StringComparer.Ordinal);
        foreach (var capability in next)
        {
            ClusterCapabilities.Validate(capability);
            if (capability.StartsWith(ClusterCapabilities.UnitPrefix, StringComparison.Ordinal))
            {
                throw new ArgumentException($"'{capability}' is reserved for unit kinds.", nameof(capabilities));
            }
        }

        await changes.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var merged = Snapshot(next);
            if (gate.Epoch is { } epoch)
            {
                using var bound = new CancellationTokenSource(options.Value.RenewTimeout, time);
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, bound.Token);
                await store.WriteAsync(options.Value.ReplicaId!, epoch, merged, linked.Token).ConfigureAwait(false);
            }

            lock (units)
            {
                application.Clear();
                application.UnionWith(next);
                current = Snapshot();
            }
        }
        finally
        {
            changes.Release();
        }
    }

    internal async Task<JoinLock> EnterJoinAsync(CancellationToken cancellationToken)
    {
        await changes.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new JoinLock(changes);
    }

    internal sealed class JoinLock(SemaphoreSlim changes) : IDisposable
    {
        private int released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref released, 1) == 0)
            {
                changes.Release();
            }
        }
    }

    private IReadOnlyList<string> Snapshot() => Snapshot(application);

    private string[] Snapshot(IEnumerable<string> applicationSet)
    {
        lock (units)
        {
            var all = applicationSet.Concat(units).Distinct(StringComparer.Ordinal).ToList();
            all.Sort(StringComparer.Ordinal);
            return [.. all];
        }
    }
}
