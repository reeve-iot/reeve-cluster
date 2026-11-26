using Int.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Migrations;

namespace Int.Log;

internal sealed class LogRig
{
    private LogRig(string connectionString, IFaultPoints faultPoints)
    {
        ConnectionString = connectionString;
        Refusal = new CommitRefusal(Gate, FenceSteps);
        Scopes = new ClusterScopeSource(FenceSteps);
        Log = new ChangeLog(Scopes, Refusal, EfTransactions, faultPoints, Events);
    }

    public string ConnectionString { get; }

    public PhaseEvents Gate { get; } = new();

    public FenceSteps FenceSteps { get; } = new();

    public ClusterScopeSource Scopes { get; }

    public CommitRefusal Refusal { get; }

    public EfTransactions EfTransactions { get; } = new();

    public RecordedEvents Events { get; } = new();

    public ChangeLog Log { get; }

    public static Task<LogRig> CreateAsync(PostgresFixture postgres, IFaultPoints? faultPoints = null) =>
        CreateAsync(postgres.ConnectionString, faultPoints);

    public static async Task<LogRig> CreateAsync(string serverConnectionString, IFaultPoints? faultPoints = null)
    {
        var connectionString = await PostgresDatabases.CreateDatabaseAsync(serverConnectionString);
        await ClusterRunners.For(connectionString)
            .MigrateAsync();

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand("CREATE TABLE public.config (id int PRIMARY KEY, value bigint NOT NULL); INSERT INTO public.config SELECT g, 0 FROM generate_series(1, 64) g;", connection);
            await command.ExecuteNonQueryAsync();
        }

        return new LogRig(connectionString, faultPoints ?? NoFaultPoints.Instance);
    }

    public async Task RunFenceStep2Async()
    {
        Gate.MoveTo(ReplicaPhase.Fencing);
        await new AppendRefusalParticipant(Refusal).RunAsync(new FenceContext(new FenceRequest(FenceTrigger.FenceStart)), CancellationToken.None);
    }

    public async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public static async Task TouchAsync(NpgsqlTransaction transaction, int id)
    {
        await using var command = new NpgsqlCommand("UPDATE public.config SET value = value + 1 WHERE id = @id", transaction.Connection, transaction);
        command.Parameters.AddWithValue("id", id);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<T> ScalarAsync<T>(string sql)
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync())!;
    }

    public async Task<List<long>> SeqsAsync()
    {
        await using var connection = await OpenAsync();
        return await SeqsAsync(connection);
    }

    public static async Task<List<long>> SeqsAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand("SELECT seq FROM reeve_cluster.change_log ORDER BY seq", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var seqs = new List<long>();
        while (await reader.ReadAsync())
        {
            seqs.Add(reader.GetInt64(0));
        }

        return seqs;
    }

    public async Task<bool> SomeoneWaitsForTheLockAsync() =>
        await ScalarAsync<long>(
            $"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND ((classid::bigint << 32) | objid::bigint) = {ChangeLogLock.Key}") > 0;

    public async Task UntilSomeoneWaitsForTheLockAsync(TimeSpan bound)
    {
        var deadline = DateTime.UtcNow + bound;
        while (!await SomeoneWaitsForTheLockAsync())
        {
            Assert.True(DateTime.UtcNow < deadline, "Nobody waited for the change-log lock.");
            await Task.Delay(10);
        }
    }

    public static ChangeEntry[] Entries(params string[] types) => [.. types.Select(t => new ChangeEntry(t, "k-" + t))];
}

internal sealed class PhaseEvents : IPhaseGate
{
    private readonly List<Action<PhaseEvent>> handlers = [];

    public ReplicaPhase Phase { get; private set; } = ReplicaPhase.Serving;

    public long? FenceStartTimestamp => null;

    public bool ExitAfterFence => false;

    public IReadinessReasons Readiness => throw new NotSupportedException();

    public void MoveTo(ReplicaPhase to)
    {
        var from = Phase;
        Phase = to;
        foreach (var handler in handlers.ToList())
        {
            handler(new PhaseEvent(PhaseEventKind.Transition, from, to));
        }
    }

    public bool DrainStep4Passed { get; private set; }

    public void RaiseDrainStep4()
    {
        DrainStep4Passed = true;
        foreach (var handler in handlers.ToList())
        {
            handler(new PhaseEvent(PhaseEventKind.DrainStep4, Phase, Phase));
        }
    }

    public IDisposable Subscribe(Action<PhaseEvent> handler)
    {
        handlers.Add(handler);
        return new Nothing();
    }

    public ServingCheckResult CheckServing() => throw new NotSupportedException();

    public FenceDecision RequestFence(FenceRequest request) => throw new NotSupportedException();

    public FenceDecision ReportFatalFault(string reason) => throw new NotSupportedException();

    public IDisposable OnStepZero(Action<FenceRequest> hook) => throw new NotSupportedException();

    public IDisposable OnFenceRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

    public IDisposable OnExitRequested(Action<FenceRequest> handler) => throw new NotSupportedException();

    private sealed class Nothing : IDisposable
    {
        public void Dispose()
        {
        }
    }
}

internal sealed class RecordedEvents : IModelEventSink
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<ModelEvent> events = new();

    public IReadOnlyList<ModelEvent> All => [.. events];

    public void Emit(in ModelEvent modelEvent) => events.Enqueue(modelEvent);
}

internal sealed class PointProbe : IFaultPoints
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> reached = new();
    private readonly TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private string? held;

    public IReadOnlyList<string> Reached => [.. reached];

    public Task Arrived => arrived.Task;

    public void Hold(FaultPoint point) => held = point.Name;

    public void Release() => released.TrySetResult();

    public async ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        reached.Enqueue(point);
        if (point == held)
        {
            arrived.TrySetResult();
            await released.Task.WaitAsync(cancellationToken);
        }
    }
}
