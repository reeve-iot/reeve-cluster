using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Reeve.Cluster.Harness.Linearizability;

internal enum Outcome
{
    Ok,

    Unknown,

    Failed,
}

internal sealed record HistoryCounts(int Writes, int UnknownWrites, int FailedWrites, int Reads, int FailedReads);

internal sealed class HistoryRecorder : IDisposable
{
    private static readonly JsonSerializerOptions Json = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    private readonly StreamWriter file;
    private readonly object gate = new();
    private long lastId;
    private readonly Dictionary<int, int> readsOf = [];
    private int writes, unknownWrites, failedWrites, reads, failedReads;

    public HistoryRecorder(string path, IReadOnlyList<string> types, IReadOnlyDictionary<string, int>? initial = null)
    {
        file = new StreamWriter(path, append: false) { AutoFlush = false };
        file.WriteLine(JsonSerializer.Serialize(new { format = "reeve-history/1", types, initial }, Json));
    }

    public HistoryCounts Counts
    {
        get
        {
            lock (gate)
            {
                return new HistoryCounts(writes, unknownWrites, failedWrites, reads, failedReads);
            }
        }
    }

    public int ReadsOf(int client)
    {
        lock (gate)
        {
            return readsOf.GetValueOrDefault(client);
        }
    }

    public static long Now() => (long)(Stopwatch.GetTimestamp() * (1_000_000_000.0 / Stopwatch.Frequency));

    public Pending BeginWrite(int client, int replica) => new(Interlocked.Increment(ref lastId), "write", client, replica, Now());

    public Pending BeginRead(int client, int replica) => new(Interlocked.Increment(ref lastId), "read", client, replica, Now());

    public void EndWrite(Pending write, Outcome outcome, IReadOnlyDictionary<string, int> values)
    {
        var ret = Now();
        lock (gate)
        {
            switch (outcome)
            {
                case Outcome.Ok:
                    writes++;
                    break;
                case Outcome.Unknown:
                    unknownWrites++;
                    break;
                default:
                    failedWrites++;
                    break;
            }

            Append(write, ret, outcome.ToString().ToLowerInvariant(), values, null, null);
        }
    }

    public void EndRead(Pending read, IReadOnlyDictionary<string, int>? values, IReadOnlyCollection<string>? skipped = null)
    {
        var ret = Now();
        lock (gate)
        {
            if (values is null)
            {
                failedReads++;
            }
            else
            {
                reads++;
                readsOf[read.Client] = readsOf.GetValueOrDefault(read.Client) + 1;
            }

            Append(read, ret, values is null ? "failed" : "ok", null, values, skipped is { Count: > 0 } ? skipped : null);
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            file.Dispose();
        }
    }

    private void Append(Pending operation, long ret, string status, IReadOnlyDictionary<string, int>? written, IReadOnlyDictionary<string, int>? read, IReadOnlyCollection<string>? skipped) =>
        file.WriteLine(JsonSerializer.Serialize(
            new
            {
                id = operation.Id,
                client = operation.Client,
                replica = operation.Replica,
                kind = operation.Kind,
                call = operation.Call,
                ret,
                status,
                writes = written,
                reads = read,
                skipped,
            },
            Json));

    internal readonly record struct Pending(long Id, string Kind, int Client, int Replica, long Call);
}
