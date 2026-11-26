using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Log;

internal sealed partial class ApplierDataSource : IAsyncDisposable
{
    public const int DefaultCommandTimeout = 30;

    public ApplierDataSource(IOptions<ClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var timeout = DefaultCommandTimeout;
        Source = new ClusterDataSource(options.Value, builder =>
        {
            builder.Pooling = false;
            if (builder.CommandTimeout <= 0)
            {
                builder.CommandTimeout = DefaultCommandTimeout;
            }

            timeout = builder.CommandTimeout;
        });
        CommandTimeout = timeout;
    }

    public ClusterDataSource Source { get; }

    public int CommandTimeout { get; }

    public NpgsqlConnection CreateConnection() => Source.DataSource.CreateConnection();

    public async Task<ISnapshotSession> OpenSnapshotSessionAsync(string snapshotId, CancellationToken cancellationToken)
    {
        if (!SnapshotId().IsMatch(snapshotId))
        {
            throw new ArgumentException($"'{snapshotId}' is not an exported snapshot's id.", nameof(snapshotId));
        }

        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);
            await using (var command = new NpgsqlCommand($"SET TRANSACTION READ ONLY; SET TRANSACTION SNAPSHOT '{snapshotId}'", connection, transaction))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            return new SnapshotSession(connection, transaction);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public ValueTask DisposeAsync() => Source.DisposeAsync();

    [GeneratedRegex("^[0-9A-F]+(-[0-9A-F]+)+$", RegexOptions.CultureInvariant)]
    private static partial Regex SnapshotId();

    private sealed class SnapshotSession(NpgsqlConnection connection, NpgsqlTransaction transaction) : ISnapshotSession
    {
        public DbConnection Connection => connection;

        public DbTransaction Transaction => transaction;

        public async ValueTask DisposeAsync()
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
