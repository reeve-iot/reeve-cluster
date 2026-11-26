using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;

#pragma warning disable CA2007

namespace Reeve.Cluster.Migrations;

internal sealed partial class ClusterMigrationRunner : IClusterMigrationRunner
{
    public const long AdvisoryLockKey = 0x524545564D494752;

    private const int CancellationTimeoutMilliseconds = 10_000;
    private const int TcpKeepAliveIdleSeconds = 10;
    private const int TcpKeepAliveIntervalSeconds = 5;
    private const int TcpKeepAliveCount = 3;
    private const int TcpUserTimeoutMilliseconds = 30_000;
    private const int ClientConnectionCheckMilliseconds = 10_000;
    private static readonly string ServerKeepAliveSql = string.Create(CultureInfo.InvariantCulture, $"SET tcp_keepalives_idle = {TcpKeepAliveIdleSeconds}; SET tcp_keepalives_interval = {TcpKeepAliveIntervalSeconds}; SET tcp_keepalives_count = {TcpKeepAliveCount}; SET tcp_user_timeout = {TcpUserTimeoutMilliseconds}; SET client_connection_check_interval = {ClientConnectionCheckMilliseconds}");

    private readonly IOptions<ClusterMigrationOptions> options;
    private readonly ILogger<ClusterMigrationRunner> logger;

    public ClusterMigrationRunner(IOptions<ClusterMigrationOptions> options, ILogger<ClusterMigrationRunner> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        this.options = options;
        this.logger = logger;
    }

    [RequiresUnreferencedCode(ClusterMigrationsDbContext.Unsupported)]
    [RequiresDynamicCode(ClusterMigrationsDbContext.Unsupported)]
    public async Task<ClusterMigrationResult> MigrateAsync(CancellationToken cancellationToken = default)
    {
        var connectionString = options.Value.ConnectionString
            ?? throw new InvalidOperationException("ClusterMigrationOptions.ConnectionString is required.");

        var statementTimeout = new NpgsqlConnectionStringBuilder(connectionString).CommandTimeout;

        await using var source = new ClusterDataSource(new ClusterOptions { ConnectionString = connectionString }, builder =>
        {
            builder.CommandTimeout = 0;
            builder.CancellationTimeout = CancellationTimeoutMilliseconds;
            builder.TcpKeepAlive = true;
            builder.TcpKeepAliveTime = TcpKeepAliveIdleSeconds;
            builder.TcpKeepAliveInterval = TcpKeepAliveIntervalSeconds;
        });

        await using var connection = await source.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var keepAlive = Statement(ServerKeepAliveSql, connection, statementTimeout))
        {
            await keepAlive.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var versionCommand = Statement("SELECT current_setting('server_version_num')::int", connection, statementTimeout))
        {
            var serverVersionNum = (int)(await versionCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
            if (PostgreSqlVersion.Refusal(serverVersionNum) is { } refusal)
            {
                throw refusal;
            }
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using (var lockCommand = Statement("SELECT pg_advisory_xact_lock(@key)", connection, statementTimeout, transaction))
        {
            lockCommand.Parameters.AddWithValue("key", AdvisoryLockKey);
            await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var context = new ClusterMigrationsDbContext(ClusterMigrationsDesignTimeFactory.Options(source.DataSource, statementTimeout));
        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var contextConnection = (NpgsqlConnection)context.Database.GetDbConnection();
            await using (var keepAlive = Statement(ServerKeepAliveSql, contextConnection, statementTimeout))
            {
                await keepAlive.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var pending = (await context.Database.GetPendingMigrationsAsync(cancellationToken).ConfigureAwait(false)).ToList();
            if (pending.Count > 0)
            {
                await context.Database.MigrateAsync(cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();

            var last = (await context.Database.GetAppliedMigrationsAsync(cancellationToken).ConfigureAwait(false)).LastOrDefault();
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            LogRun(logger, pending.Count, last ?? "none");
            return new ClusterMigrationResult(pending, last);
        }
        finally
        {
            await context.Database.CloseConnectionAsync().ConfigureAwait(false);
        }
    }

    private static NpgsqlCommand Statement(string sql, NpgsqlConnection connection, int statementTimeout, NpgsqlTransaction? transaction = null) =>
        new(sql, connection, transaction) { CommandTimeout = statementTimeout };

    [LoggerMessage(Level = LogLevel.Information, Message = "reeve_cluster migrations: applied {Applied}, last migration {LastMigration}")]
    private static partial void LogRun(ILogger logger, int applied, string lastMigration);
}
