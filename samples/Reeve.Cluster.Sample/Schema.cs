using Npgsql;

namespace Reeve.Cluster.Sample;

internal static class SampleSchema
{
    public static async Task EnsureAsync(string connectionString, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            SELECT pg_advisory_xact_lock(hashtext('reeve-cluster-sample-schema'));
            CREATE TABLE IF NOT EXISTS sample_flags (name text PRIMARY KEY, enabled boolean NOT NULL);
            CREATE TABLE IF NOT EXISTS sample_quotas (tenant text PRIMARY KEY, daily_limit integer NOT NULL);
            CREATE TABLE IF NOT EXISTS sample_settings (name text PRIMARY KEY, value integer NOT NULL);
            """,
            connection, transaction) { CommandTimeout = 30 };
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }
}
