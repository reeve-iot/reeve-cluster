using Npgsql;

namespace Reeve.Cluster.Core;

internal sealed class ClusterDataSource : IAsyncDisposable
{
    private readonly NpgsqlDataSource dataSource;

    public ClusterDataSource(ClusterOptions options)
        : this(options, static _ => { })
    {
    }

    public ClusterDataSource(ClusterOptions options, Action<NpgsqlConnectionStringBuilder> tune)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(tune);

        var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
        tune(builder.ConnectionStringBuilder);
        builder.UsePhysicalConnectionInitializer(Check, CheckAsync);
        dataSource = builder.Build();
    }

    public NpgsqlDataSource DataSource => dataSource;

    public ValueTask DisposeAsync() => dataSource.DisposeAsync();

    private static int ProbeTimeout(NpgsqlConnection connection) => connection.ConnectionTimeout;

    private static void Check(NpgsqlConnection connection)
    {
        using var command = new NpgsqlCommand(PhysicalConnectionCheck.ProbeSql, connection) { CommandTimeout = ProbeTimeout(connection) };
        using var reader = command.ExecuteReader();
        reader.Read();
        Throw(PhysicalConnectionCheck.Evaluate(reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2)));
    }

    private static async Task CheckAsync(NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand(PhysicalConnectionCheck.ProbeSql, connection) { CommandTimeout = ProbeTimeout(connection) };
        try
        {
            var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            try
            {
                await reader.ReadAsync().ConfigureAwait(false);
                Throw(PhysicalConnectionCheck.Evaluate(reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2)));
            }
            finally
            {
                await reader.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            await command.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void Throw(ClusterConfigurationException? fault)
    {
        if (fault is not null)
        {
            throw fault;
        }
    }
}
