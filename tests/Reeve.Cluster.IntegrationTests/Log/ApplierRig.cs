using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

namespace Int.Log;

internal sealed class ApplierRig : IAsyncDisposable
{
    private ApplierRig(LogRig log, ClusterOptions options, ProbeReconciler[] reconcilers, IFaultPoints points, TimeProvider time, ILogger<Applier> applierLogger)
    {
        Log = log;
        Time = time;
        Options = options;
        Reconcilers = reconcilers;
        var wrapped = Microsoft.Extensions.Options.Options.Create(options);
        Gate = new LocalPhaseGate(wrapped, time, Events, points, NullLogger<LocalPhaseGate>.Instance);
        Store = new GenerationStore(ReconcilerGraph.Build(reconcilers), points, Events, time, NullLogger.Instance);
        Passes = new ApplierDataSource(wrapped);
        Data = new ClusterDataSource(options);
        Members = new MemberStore(new RenewalDataSource(wrapped), new MemberWriteDataSource(wrapped));
        Channel = new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance);
        Applier = new Applier(
            Store, reconcilers, Gate, PassConnection, Passes, Data, Channel, log.Scopes, wrapped, time, points, Events,
            applierLogger);
        foreach (var reconciler in reconcilers)
        {
            reconciler.Scopes = log.Scopes;
        }
    }

    public LogRig Log { get; }

    public TimeProvider Time { get; }

    public ClusterOptions Options { get; }

    public ProbeReconciler[] Reconcilers { get; }

    public RecordedEvents Events { get; } = new();

    public LocalPhaseGate Gate { get; }

    public GenerationStore Store { get; }

    public ApplierDataSource Passes { get; }

    public ClusterDataSource Data { get; }

    public MemberStore Members { get; }

    public InProcessWakeupChannel Channel { get; }

    public FencePassConnection PassConnection { get; } = new();

    public Applier Applier { get; }

    public long Epoch { get; private set; }

    public long Timeline { get; private set; }

    public ProbeReconciler this[string type] => Reconcilers.Single(r => r.AggregateType == type);

    public static async Task<ApplierRig> CreateAsync(
        PostgresFixture postgres, string[] types, IFaultPoints? points = null, Action<ClusterOptions>? configure = null, TimeProvider? time = null,
        ILogger<Applier>? applierLogger = null, IReadOnlyDictionary<string, string[]>? dependsOn = null)
    {
        var log = await LogRig.CreateAsync(postgres);
        var options = new ClusterOptions();
        TestProfile.Apply(options);
        options.ReplicaId = "r1";
        options.ConnectionString = log.ConnectionString;
        options.LeaseTtl = TimeSpan.FromMinutes(10);
        options.ReconcileTimeout = TimeSpan.FromSeconds(10);
        configure?.Invoke(options);
        return new ApplierRig(log, options, [.. types.Select((t, i) => new ProbeReconciler(t, i + 1) { DependsOnTypes = dependsOn?.GetValueOrDefault(t) ?? [] })], points ?? NoFaultPoints.Instance, time ?? TimeProvider.System,
            applierLogger ?? NullLogger<Applier>.Instance);
    }

    public async Task JoinAsync(long? joinedTimeline = null)
    {
        var identity = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var cursor = Applier.Cursor;
        var answer = await Members.JoinAsync(
            new JoinRequest("r1", Options.LeaseTtl, identity, cursor.Seq, cursor.Token), CancellationToken.None);
        Assert.NotNull(answer);
        Epoch = answer.Value.Epoch;
        Timeline = answer.Value.Timeline;
        Assert.True(Gate.Joined(Epoch, joinedTimeline ?? Timeline, Gate.Now));
    }

    public async Task LoadAsync(bool startLoop = false)
    {
        await JoinAsync();
        if (startLoop)
        {
            Assert.Equal(Reeve.Cluster.Membership.LoadOutcome.Loaded, await Applier.LoadAsync(Epoch, Timeline, CancellationToken.None));
            Applier.StartApplying(Epoch);
        }
        else
        {
            Assert.Equal(PassOutcome.Published, await PassAsync());
        }

        Assert.Equal(ReplicaPhase.Loading, Gate.Phase);
    }

    public Task<PassOutcome> PassAsync(bool withoutHints = false) => Applier.RunPassAsync(Epoch, forceFull: false, withoutHints, CancellationToken.None);

    public async Task<long> WriteAsync(params (string Type, string? Key)[] changes)
    {
        await using var connection = await Log.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        long seq = 0;
        foreach (var (type, key) in changes)
        {
            if (Reconcilers.FirstOrDefault(r => r.AggregateType == type) is { } reconciler)
            {
                await LogRig.TouchAsync(transaction, reconciler.ConfigId);
            }

            await using var command = new NpgsqlCommand(
                "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES (@t, @k) RETURNING seq", connection, transaction);
            command.Parameters.AddWithValue("t", type);
            command.Parameters.Add(new NpgsqlParameter("k", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)key ?? DBNull.Value });
            seq = Math.Max(seq, (long)(await command.ExecuteScalarAsync())!);
        }

        await transaction.CommitAsync();
        return seq;
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<Cursor> RowCursorAsync()
    {
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT applied_seq, applied_token FROM reeve_cluster.member WHERE replica_id = 'r1'", connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return new Cursor(reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetGuid(1));
    }

    public async Task<Cursor> HeadAsync()
    {
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT seq, entry_token FROM reeve_cluster.change_log ORDER BY seq DESC LIMIT 1", connection);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() ? new Cursor(reader.GetInt64(0), reader.GetGuid(1)) : Cursor.Empty;
    }

    public ProbeState State(string type) => Store.Current!.Get<ProbeState>(type);

    public async ValueTask DisposeAsync()
    {
        Applier.Dispose();
        Gate.Dispose();
        await Passes.DisposeAsync();
        await Data.DisposeAsync();
    }
}

internal sealed record ProbeState(
    long Head, long Value, bool FullLoad, IReadOnlySet<string>? Hints, ClusterScopeKind? Scope, IReadOnlyDictionary<string, object> Dependencies);

internal sealed class ProbeReconciler(string type, int configId) : IReconciler
{
    public string AggregateType => type;

    public int ConfigId => configId;

    public IReadOnlyList<string> DependsOnTypes { get; init; } = [];

    IReadOnlyList<string> IReconciler.DependsOn => DependsOnTypes;

    public IClusterScopeAccessor? Scopes { get; set; }

    public bool ReadThroughOwnSession { get; set; }

    public Func<ReconcileContext, CancellationToken, Task>? Before { get; set; }

    public ConcurrentQueue<ReconcileContext> Calls { get; } = new();

    public async Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken)
    {
        Calls.Enqueue(context);
        if (Before is { } before)
        {
            await before(context, cancellationToken);
        }

        var scope = Scopes?.Current?.Kind;
        long value;
        if (ReadThroughOwnSession)
        {
            await using var session = await context.OpenSnapshotSessionAsync(cancellationToken);
            value = await ReadAsync((NpgsqlConnection)session.Connection, (NpgsqlTransaction)session.Transaction, cancellationToken);
        }
        else
        {
            value = await ReadAsync((NpgsqlConnection)context.Connection, (NpgsqlTransaction)context.Transaction, cancellationToken);
        }

        return new ReconcileResult(new ProbeState(
            context.Head, value, context.FullLoad, context.Hints, scope, context.Dependencies.ToDictionary(d => d, context.GetDependency)));
    }

    private async Task<long> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT value FROM public.config WHERE id = @id", connection, transaction);
        command.Parameters.AddWithValue("id", configId);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
