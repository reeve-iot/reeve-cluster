using Npgsql;
using Xunit;

namespace Reeve.Cluster.Harness.Schema;

public static class SchemaAssertions
{
    public const string SchemaName = "reeve_cluster";

    public static async Task EverySequenceIsCacheOneAsync(NpgsqlConnection connection, string schema = SchemaName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand("SELECT sequencename, cache_size FROM pg_sequences WHERE schemaname = @schema ORDER BY sequencename", connection);
        command.Parameters.AddWithValue("schema", schema);

        var cached = new List<string>();
        var count = 0;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                if (reader.GetInt64(1) != 1)
                {
                    cached.Add($"{reader.GetString(0)} (CACHE {reader.GetInt64(1)})");
                }
            }
        }

        Assert.True(count > 0, $"Schema {schema} has no sequence; the CACHE 1 rule was checked over nothing.");
        Assert.True(cached.Count == 0, $"Every sequence of {schema} must be CACHE 1; these are not: {string.Join(", ", cached)}.");
    }
}
