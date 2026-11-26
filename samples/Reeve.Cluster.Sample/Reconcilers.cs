using Npgsql;

namespace Reeve.Cluster.Sample;

public sealed record FlagState(IReadOnlyDictionary<string, bool> Flags);

public sealed record QuotaState(IReadOnlyDictionary<string, int> Limits);

public sealed class FlagReconciler : IReconciler
{
    public string AggregateType => "flag";

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var hinted = context.Hints is not null && context.Previous is FlagState;
        var flags = hinted ? new Dictionary<string, bool>(((FlagState)context.Previous!).Flags) : [];
        if (hinted)
        {
            foreach (var key in context.Hints!)
            {
                flags.Remove(key);
            }
        }

        await using var command = new NpgsqlCommand(
            hinted ? "SELECT name, enabled FROM sample_flags WHERE name = ANY(@keys)" : "SELECT name, enabled FROM sample_flags",
            (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction) { CommandTimeout = 5 };
        if (hinted)
        {
            command.Parameters.AddWithValue("keys", context.Hints!.ToArray());
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            flags[reader.GetString(0)] = reader.GetBoolean(1);
        }

        return new ReconcileResult(new FlagState(flags));
    }
}

public sealed class QuotaReconciler : IReconciler
{
    public string AggregateType => "quota";

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var limits = new Dictionary<string, int>();
        await using var command = new NpgsqlCommand(
            "SELECT tenant, daily_limit FROM sample_quotas",
            (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction) { CommandTimeout = 5 };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            limits[reader.GetString(0)] = reader.GetInt32(1);
        }

        return new ReconcileResult(new QuotaState(limits));
    }
}

public sealed record SettingState(IReadOnlyDictionary<string, int> Values);

public sealed class SettingReconciler : IReconciler
{
    public string AggregateType => "setting";

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, int>();
        await using var command = new NpgsqlCommand(
            "SELECT name, value FROM sample_settings",
            (NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction) { CommandTimeout = 5 };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            values[reader.GetString(0)] = reader.GetInt32(1);
        }

        return new ReconcileResult(new SettingState(values));
    }
}
