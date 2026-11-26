using System.Reflection;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Membership;

internal sealed record MemberIdentity(Guid ProcessId, string Version, int Protocol, IReadOnlyList<string> Capabilities)
{
    public const int CurrentProtocol = 1;

    public const int UpgradePathProtocol = 1;

    public static MemberIdentity ForThisProcess()
    {
        var assembly = Assembly.GetEntryAssembly();
        var version = assembly?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        return Create(Guid.NewGuid(), version, assembly?.GetName().Name ?? "unknown");
    }

    internal static MemberIdentity Create(Guid processId, string? version, string assemblyName)
    {
        var informationalVersion = string.IsNullOrWhiteSpace(version) ? "0.0.0" : version;
        if (!SemanticVersion.TryParse(informationalVersion, out _))
        {
            throw new ClusterConfigurationException(
                ClusterConfigurationReason.VersionNotSemVer,
                $"The informational version '{informationalVersion}' of assembly '{assemblyName}' is not SemVer 2.0.");
        }

        return new MemberIdentity(processId, informationalVersion, CurrentProtocol, []);
    }
}

internal sealed record JoinRequest(string ReplicaId, TimeSpan LeaseTtl, MemberIdentity Identity, long AppliedSeq = 0, Guid? AppliedToken = null);

internal readonly record struct JoinAnswer(long Epoch, long Timeline);

internal interface IMemberStore
{
    Task<JoinAnswer?> JoinAsync(JoinRequest request, CancellationToken cancellationToken);

    Task<RenewalAnswer> RenewAsync(string replicaId, long epoch, TimeSpan leaseTtl, CancellationToken cancellationToken);

    Task<bool> WriteDrainingAsync(string replicaId, long epoch, CancellationToken cancellationToken);

    Task<bool> ClearServingSinceAsync(string replicaId, long epoch, CancellationToken cancellationToken);
}

internal sealed class RenewalDataSource : IAsyncDisposable
{
    public RenewalDataSource(IOptions<ClusterOptions> options)
    {
        var o = options.Value;
        Source = new ClusterDataSource(o, builder =>
        {
            builder.ApplicationName = "reeve-cluster renewal";
            builder.TcpKeepAlive = true;
            builder.TcpKeepAliveTime = Math.Max(1, (int)o.RenewInterval.TotalSeconds);
            builder.TcpKeepAliveInterval = Math.Max(1, (int)o.RenewTimeout.TotalSeconds);
            builder.CancellationTimeout = Math.Max(1, (int)o.RenewTimeout.TotalMilliseconds);
            builder.MaxPoolSize = 8;
            builder.CommandTimeout = 0;
        });
    }

    public ClusterDataSource Source { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed class MemberWriteDataSource : IAsyncDisposable
{
    public MemberWriteDataSource(IOptions<ClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var o = options.Value;
        Source = new ClusterDataSource(o, builder =>
        {
            builder.CommandTimeout = 0;

            builder.CancellationTimeout = Math.Max(1, (int)o.RenewTimeout.TotalMilliseconds);
        });
    }

    public ClusterDataSource Source { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed class MemberStore(RenewalDataSource renewal, MemberWriteDataSource writes) : IMemberStore
{
    internal const string TimelineSql = "('x' || lpad(substr(pg_walfile_name(pg_current_wal_insert_lsn()), 1, 8), 16, '0'))::bit(64)::bigint";

    internal const string JoinSql = $"""
        INSERT INTO reeve_cluster.member AS m
            (replica_id, epoch, state, lease_expires_at, serving_since, activated_at_seq, applied_seq, applied_token,
             capabilities, version, protocol, process_id)
        VALUES (@id, nextval('reeve_cluster.member_epoch'), 'joining', clock_timestamp() + @ttl, NULL, NULL,
                @applied_seq, @applied_token, @capabilities, @version, @protocol, @process_id)
        ON CONFLICT (replica_id) DO UPDATE SET
            epoch = EXCLUDED.epoch, state = 'joining', lease_expires_at = EXCLUDED.lease_expires_at, serving_since = NULL,
            activated_at_seq = NULL, applied_seq = EXCLUDED.applied_seq, applied_token = EXCLUDED.applied_token,
            capabilities = EXCLUDED.capabilities, version = EXCLUDED.version, protocol = EXCLUDED.protocol,
            process_id = EXCLUDED.process_id
        WHERE m.state IN ('fenced', 'draining') OR m.lease_expires_at <= clock_timestamp()
        RETURNING m.epoch, {TimelineSql}
        """;

    internal const string RenewSql = $"""
        UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() + @ttl
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active') AND lease_expires_at > clock_timestamp()
        RETURNING {TimelineSql}
        """;

    internal const string DrainingSql = """
        UPDATE reeve_cluster.member SET state = 'draining', serving_since = NULL
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active')
        """;

    internal const string ClearServingSinceSql = """
        UPDATE reeve_cluster.member SET serving_since = NULL
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active')
        """;

    public async Task<JoinAnswer?> JoinAsync(JoinRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(JoinSql, connection);
        command.Parameters.AddWithValue("id", request.ReplicaId);
        command.Parameters.AddWithValue("ttl", request.LeaseTtl);
        command.Parameters.AddWithValue("applied_seq", request.AppliedSeq);
        command.Parameters.Add(new NpgsqlParameter("applied_token", NpgsqlDbType.Uuid) { Value = (object?)request.AppliedToken ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter("capabilities", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = request.Identity.Capabilities.ToArray() });
        command.Parameters.AddWithValue("version", request.Identity.Version);
        command.Parameters.AddWithValue("protocol", request.Identity.Protocol);
        command.Parameters.AddWithValue("process_id", request.Identity.ProcessId);

        JoinAnswer? answer = null;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                answer = new JoinAnswer(reader.GetInt64(0), reader.GetInt64(1));
            }

            while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        return answer;
    }

    public async Task<RenewalAnswer> RenewAsync(string replicaId, long epoch, TimeSpan leaseTtl, CancellationToken cancellationToken)
    {
        await using var connection = await renewal.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(RenewSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        command.Parameters.AddWithValue("ttl", leaseTtl);

        long? timeline = null;
        var rows = 0;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows++;
                timeline = reader.GetInt64(0);
            }

            while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        return new RenewalAnswer(rows, timeline);
    }

    public Task<bool> WriteDrainingAsync(string replicaId, long epoch, CancellationToken cancellationToken) =>
        WriteOwnRowAsync(DrainingSql, replicaId, epoch, cancellationToken);

    public Task<bool> ClearServingSinceAsync(string replicaId, long epoch, CancellationToken cancellationToken) =>
        WriteOwnRowAsync(ClearServingSinceSql, replicaId, epoch, cancellationToken);

    private async Task<bool> WriteOwnRowAsync(string sql, string replicaId, long epoch, CancellationToken cancellationToken)
    {
        await using var connection = await writes.Source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();
        return rows == 1;
    }
}
