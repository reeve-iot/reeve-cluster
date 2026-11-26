using System.Collections.Frozen;
using System.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

#pragma warning disable CA2007

namespace Reeve.Cluster.Log;

internal enum PassOutcome
{
    Published,

    NothingNew,

    Stalled,

    Abandoned,

    OtherTimeline,

    Failed,
}

internal sealed record ApplierStatus(Cursor Cursor, long? Generation, long? ObservedHead, TimeSpan? StalledFor, string? FailingType, long FirstUnappliedSeq);

internal sealed class Applier : IMemberLoad, IDisposable
{
    public const string LoadFailedReason = "loading.failed";

    internal const string SnapshotSql = $"""
        SET TRANSACTION READ ONLY;
        SELECT {MemberStore.TimelineSql}, pg_export_snapshot(), reeve_cluster.cursor_valid(@seq, @token), h.seq, h.entry_token
        FROM (SELECT 1) AS one
        LEFT JOIN LATERAL (SELECT seq, entry_token FROM reeve_cluster.change_log ORDER BY seq DESC LIMIT 1) AS h ON true
        """;

    internal const string FullSql = """
        SELECT (SELECT count(*) FROM reeve_cluster.change_log),
               array(SELECT k.aggregate_type
                     FROM (SELECT aggregate_type, min(from_seq) AS from_seq FROM reeve_cluster.change_skip
                           WHERE @keep AND until_seq IS NULL GROUP BY aggregate_type) AS k
                     WHERE k.from_seq <= (SELECT max(seq) FROM reeve_cluster.change_log)
                       AND NOT EXISTS (SELECT 1 FROM reeve_cluster.change_log l
                                       WHERE l.aggregate_type = k.aggregate_type AND l.seq > @seq AND l.seq < k.from_seq))
        """;

    internal const string DirtySql = """
        SELECT aggregate_type, count(*), count(*) FILTER (WHERE NOT covered),
               count(DISTINCT aggregate_key) FILTER (WHERE NOT covered), bool_or(aggregate_key IS NULL AND NOT covered),
               (array_agg(DISTINCT aggregate_key) FILTER (WHERE aggregate_key IS NOT NULL AND NOT covered))[1:@limit]
        FROM (SELECT l.aggregate_type, l.aggregate_key,
                     EXISTS (SELECT 1 FROM reeve_cluster.change_skip s
                             WHERE s.aggregate_type = l.aggregate_type AND s.from_seq <= l.seq
                               AND (s.until_seq IS NULL OR l.seq < s.until_seq)) AS covered
              FROM reeve_cluster.change_log l WHERE l.seq > @seq) AS e
        GROUP BY aggregate_type
        """;

    internal const string PollSql = """
        SELECT h.seq, h.entry_token, (SELECT entry_token FROM reeve_cluster.change_log WHERE seq = @seq),
               (SELECT horizon_seq FROM reeve_cluster.cluster)
        FROM (SELECT 1) AS one
        LEFT JOIN LATERAL (SELECT seq, entry_token FROM reeve_cluster.change_log ORDER BY seq DESC LIMIT 1) AS h ON true
        """;

    internal const string CursorSql = """
        UPDATE reeve_cluster.member SET applied_seq = @seq, applied_token = @token
        WHERE replica_id = @id AND epoch = @epoch AND state IN ('joining', 'active')
        """;

    private const string SkipHint =
        "If the cause is the data, open a skip of each of {SkipTypes}, the dependencies first and {SkippedType} last, each with fromSeq = ackedSeq + 1 " +
        "(send fromSeq 1; if it is refused with 409 skip_covers_acknowledged, resend with ackedSeq + 1, the ackedSeq being in the answer), and close none of them before the fix runs on every replica.";

    private readonly object sync = new();
    private readonly GenerationStore store;
    private readonly FrozenDictionary<string, IReconciler> reconcilers;
    private readonly LocalPhaseGate gate;
    private readonly FencePassConnection passConnection;
    private readonly ApplierDataSource passes;
    private readonly ClusterDataSource data;
    private readonly IWakeupChannel channel;
    private readonly IClusterScopeSource scopes;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly TimeSpan reconcileTimeout;
    private readonly TimeSpan pollInterval;
    private readonly int hintLimit;
    private readonly TimeSpan retryCap;
    private readonly IDisposable wakeups;
    private readonly ApplierStallClock stall;
    private readonly StuckAttemptWatch stuck;

    private long stoppedThrough;
    private ApplierPass? openPass;
    private CancellationTokenSource? loop;
    private PublishedPass? lastPublished;
    private long lastPassEntries;
    private (long Epoch, Cursor Head)? unwrittenCursor;
    private string? fullPassFailure;
    private string? failingType;
    private bool disposed;

    private long lastHead = Unread;
    private long lastHorizon = Unread;

    private TaskCompletionSource wakeup = NewSignal();

    public Applier(
        GenerationStore store,
        IEnumerable<IReconciler> reconcilers,
        LocalPhaseGate gate,
        FencePassConnection passConnection,
        ApplierDataSource passes,
        ClusterDataSource data,
        IWakeupChannel channel,
        IClusterScopeSource scopes,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        IModelEventSink events,
        ILogger<Applier> logger)
    {
        ArgumentNullException.ThrowIfNull(reconcilers);
        ArgumentNullException.ThrowIfNull(options);
        this.store = store;
        this.reconcilers = reconcilers.ToFrozenDictionary(r => r.AggregateType, StringComparer.Ordinal);
        this.gate = gate;
        this.passConnection = passConnection;
        this.passes = passes;
        this.data = data;
        this.channel = channel;
        this.scopes = scopes;
        this.time = time;
        this.points = points;
        this.events = events;
        this.logger = logger;

        var o = options.Value;
        replicaId = o.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
        reconcileTimeout = o.ReconcileTimeout ?? throw new ArgumentException("ReconcileTimeout is required.", nameof(options));
        pollInterval = o.ApplyPollInterval;
        hintLimit = o.HintLimit;

        var allowance = reconcileTimeout + o.ApplyStallLimit;
        retryCap = o.ApplyStallLimit > pollInterval ? o.ApplyStallLimit : pollInterval;
        stall = new ApplierStallClock(gate, allowance, time, points, logger);
        stuck = new StuckAttemptWatch(store, gate, allowance, pollInterval, points, logger);
        ClusterDiagnostics.TrackApplier(this);

        wakeups = channel.Subscribe(WakeupKind.Appended, _ => Wake());
    }

    internal ApplierStatus Status
    {
        get
        {
            var current = store.Current;
            string? failing;
            lock (sync)
            {
                failing = failingType;
            }

            var left = stall.Left;
            var head = LastHead;
            return new ApplierStatus(
                current is null ? Cursor.Empty : new Cursor(current.Head, current.HeadToken),
                (current as Generation)?.Id,
                head == Unread ? null : head,
                left is null ? null : stall.Limit - left,
                failing,
                (current?.Head ?? 0) + 1);
        }
    }

    public Cursor Cursor => store.Current is { } g ? new Cursor(g.Head, g.HeadToken) : Cursor.Empty;

    public PublishedPass? LastPublished
    {
        get
        {
            lock (sync)
            {
                return lastPublished;
            }
        }
    }

    public long LastPassEntries
    {
        get
        {
            lock (sync)
            {
                return lastPassEntries;
            }
        }
    }

    public async Task<LoadOutcome> LoadAsync(long epoch, long timeline, CancellationToken cancellationToken)
    {
        var started = time.GetTimestamp();
        lock (sync)
        {
            fullPassFailure = null;
        }

        var outcome = await RunPassAsync(epoch, forceFull: true, withoutHints: false, cancellationToken).ConfigureAwait(false);
        if (outcome == PassOutcome.Published)
        {
            gate.Readiness.Clear(LoadFailedReason);
            return LoadOutcome.Loaded;
        }

        if (outcome is PassOutcome.Stalled or PassOutcome.Failed && !cancellationToken.IsCancellationRequested && !Ended(epoch))
        {
            ReportFailedLoad(epoch, time.GetElapsedTime(started));
        }

        return LoadOutcome.Failed;
    }

    public void StartApplying(long epoch) => StartLoop(epoch);

    internal const long Unread = long.MinValue;

    internal long LastHead => Volatile.Read(ref lastHead);

    internal long LastHorizon => Volatile.Read(ref lastHorizon);

    public void Stop() => StopThrough(gate.Epoch ?? 0);

    public void StopForGood() => StopThrough(long.MaxValue);

    public void Dispose()
    {
        lock (sync)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
        }

        StopForGood();
        ClusterDiagnostics.UntrackApplier(this);
        wakeups.Dispose();
        stall.Dispose();
        stuck.Dispose();
    }

    internal async Task<PassOutcome> RunPassAsync(long epoch, bool forceFull, bool withoutHints, CancellationToken cancellationToken)
    {
        await points.ReachAsync(LogFaultPoints.PassBegin, cancellationToken).ConfigureAwait(false);
        if (Ended(epoch))
        {
            return PassOutcome.Abandoned;
        }

        using var scope = scopes.Begin(ClusterScopeKind.Applier);
        var lease = new PassConnection(passes.CreateConnection());
        var handOver = passConnection.Use(lease);
        ApplierPass? pass = null;
        var published = false;
        try
        {
            var outcome = await RunPassOnAsync(lease, epoch, forceFull, withoutHints, p => pass = p, cancellationToken).ConfigureAwait(false);
            published = outcome is PassOutcome.Published or PassOutcome.NothingNew;
            return outcome;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!cancellationToken.IsCancellationRequested && !Ended(epoch))
            {
                logger.LogWarning(exception, "A pass of epoch {Epoch} failed; it publishes nothing and is retried.", epoch);
            }

            return Ended(epoch) || cancellationToken.IsCancellationRequested ? PassOutcome.Abandoned : PassOutcome.Failed;
        }
        finally
        {
            if (pass is not null)
            {
                pass.Abandon();
                lock (sync)
                {
                    if (ReferenceEquals(openPass, pass))
                    {
                        openPass = null;
                    }
                }
            }

            handOver.Dispose();
            if (published)
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                _ = lease.DisposeAsync().AsTask().ContinueWith(
                    t => logger.LogDebug(t.Exception, "An abandoned pass's connection failed to close."),
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
    }

    internal void StartLoop(long epoch)
    {
        CancellationTokenSource started;
        CancellationTokenSource? previous;
        lock (sync)
        {
            if (disposed || epoch <= stoppedThrough)
            {
                return;
            }

            previous = loop;
            started = new CancellationTokenSource();
            loop = started;
        }

        _ = previous?.CancelAsync();
        _ = Task.Run(() => RunLoopAsync(epoch, started.Token), CancellationToken.None);
    }

    internal async Task<bool> PollAsync(CancellationToken cancellationToken)
    {
        var local = Cursor;
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(PollSql, connection) { CommandTimeout = passes.CommandTimeout };
        command.Parameters.AddWithValue("seq", local.Seq);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var head = reader.IsDBNull(0) ? Cursor.Empty : new Cursor(reader.GetInt64(0), reader.GetGuid(1));
        Guid? rowToken = reader.IsDBNull(2) ? null : reader.GetGuid(2);
        var horizon = reader.IsDBNull(3) ? -1 : reader.GetInt64(3);
        Volatile.Write(ref lastHead, head.Seq);
        if (horizon >= 0)
        {
            Volatile.Write(ref lastHorizon, horizon);
        }

        return ApplierPoll.PassNeeded(local, head, rowToken, horizon);
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task<PassOutcome> RunPassOnAsync(
        PassConnection lease, long epoch, bool forceFull, bool withoutHints, Action<ApplierPass> begun, CancellationToken cancellationToken)
    {
        var connection = lease.Connection;
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);

        var local = Cursor;
        var snapshot = await ReadSnapshotAsync(transaction, local, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref lastHead, snapshot.Head.Seq);
        if (!gate.CheckTimeline(epoch, snapshot.Timeline))
        {
            return Ended(epoch) || gate.Epoch != epoch ? PassOutcome.Abandoned : PassOutcome.OtherTimeline;
        }

        await points.ReachAsync(LogFaultPoints.AfterSnapshotRead, cancellationToken).ConfigureAwait(false);

        var reload = forceFull;
        var first = store.Current is null;
        var full = first || reload || !snapshot.CursorValid;
        if (!full && snapshot.Head == local)
        {
            await EndAsync(transaction, cancellationToken).ConfigureAwait(false);
            Recovered();
            return PassOutcome.NothingNew;
        }

        if (!first && !snapshot.CursorValid)
        {
            ClusterDiagnostics.InvalidCursorReloads.Add(1);
            logger.LogInformation(
                "The local cursor {Cursor} names no entry of the log, whose head is {Head}: the log was rewound or retention passed the cursor, and the pass reloads every type in full.",
                local.Seq, snapshot.Head.Seq);
        }

        if (full && !first)
        {
            await points.ReachAsync(LogFaultPoints.BeforeFullReload, cancellationToken).ConfigureAwait(false);
        }

        IReadOnlyList<string> dirty;
        IReadOnlyList<string> qualifying = [];
        IReadOnlyDictionary<string, IReadOnlySet<string>?> hints = FrozenDictionary<string, IReadOnlySet<string>?>.Empty;
        long entries;
        if (full)
        {
            var keep = reload && !first && snapshot.CursorValid;
            (entries, var skipped) = await ReadFullAsync(transaction, local, keep, cancellationToken).ConfigureAwait(false);
            qualifying = [.. store.AggregateTypes.Where(skipped.Contains)];
            dirty = qualifying.Count == 0 ? store.AggregateTypes : [.. store.AggregateTypes.Where(t => !skipped.Contains(t))];
        }
        else
        {
            (dirty, hints, entries) = await ReadDirtyAsync(transaction, local, withoutHints, cancellationToken).ConfigureAwait(false);
        }

        ApplierPass pass;
        IReadOnlyList<string> kept;
        IReadOnlyList<string> reloaded;
        long passStarted;
        lock (sync)
        {
            if (Ended(epoch))
            {
                return PassOutcome.Abandoned;
            }

            pass = store.BeginPass(snapshot.Head.Seq, snapshot.Head.Token, dirty, full);
            openPass = pass;
            var reconciling = pass.DirtyTypes.ToHashSet(StringComparer.Ordinal);
            kept = full ? [.. store.AggregateTypes.Where(t => !reconciling.Contains(t))] : [];
            reloaded = [.. qualifying.Where(reconciling.Contains)];

            passStarted = time.GetTimestamp();

            events.Emit(new ModelEvent(LogActions.PassBegin, new Dictionary<string, object?>
            {
                ["pass"] = pass.Number,
                ["epoch"] = epoch,
                ["head"] = snapshot.Head.Seq,
                ["headToken"] = snapshot.Head.Token,
                ["dirty"] = pass.DirtyTypes,
                ["kept"] = kept,
                ["entries"] = entries,
                ["full"] = full,
                ["reload"] = full && reload,
            }));
        }

        begun(pass);

        if (kept.Count > 0)
        {
            logger.LogInformation(
                "The reload keeps the state of {Kept}: an open skip covers every entry of it after the cursor {Cursor}.", kept, local.Seq);
        }

        if (reloaded.Count > 0)
        {
            logger.LogInformation(
                "The reload reconciles {Reloaded} although an open skip covers every entry of it after the cursor {Cursor}: a type it depends on is reconciled.",
                reloaded, local.Seq);
        }

        if (first)
        {
            await points.ReachAsync(LogFaultPoints.InsideFirstLoad, cancellationToken).ConfigureAwait(false);
        }

        foreach (var type in pass.DirtyTypes)
        {
            var reconciler = reconcilers[type];

            var typeHints = full || withoutHints || pass.HasDirtyDependency(type) ? null : hints.GetValueOrDefault(type);
            var previous = pass.Base?.Get(type);

            var outcome = await pass.RunAttemptAsync(
                type,
                attempt => ReconcileAsync(reconciler, new ReconcileContext(
                    type, snapshot.Head.Seq, pass.Number, attempt.Number, full, typeHints, previous, transaction,
                    ct => passes.OpenSnapshotSessionAsync(snapshot.SnapshotId, ct), pass.DependencyStates(type)), attempt.Token),
                reconcileTimeout).ConfigureAwait(false);

            switch (outcome.Kind)
            {
                case AttemptOutcomeKind.Accepted:
                    continue;
                case AttemptOutcomeKind.Failed:
                    logger.LogWarning(outcome.Error, "The reconciler of {AggregateType} failed in pass {Pass}; the pass is retried. " + SkipHint, type, pass.Number, store.Graph.ClosureOf(type), type);
                    FullPassFailed(full, $"the reconciler of {type} failed ({outcome.Error?.GetType().Name}) after {store.Time.GetElapsedTime(outcome.Attempt.Started)}");
                    Stalled(epoch, passStarted, type);
                    return PassOutcome.Stalled;
                case AttemptOutcomeKind.TimedOut:
                    logger.LogWarning("The reconciler of {AggregateType} did not return within ReconcileTimeout in pass {Pass}; its attempt is discarded and the pass is retried. " + SkipHint, type, pass.Number, store.Graph.ClosureOf(type), type);
                    FullPassFailed(full, $"the reconciler of {type} did not return within ReconcileTimeout; its attempt was discarded after {store.Time.GetElapsedTime(outcome.Attempt.Started)}");
                    Stalled(epoch, passStarted, type);
                    return PassOutcome.Stalled;
                default:
                    return PassOutcome.Abandoned;
            }
        }

        if (store.Publish(pass) is null)
        {
            return PassOutcome.Abandoned;
        }

        lock (sync)
        {
            lastPublished = new PublishedPass(snapshot.Head, entries, full);
            lastPassEntries = entries;
            unwrittenCursor = (epoch, snapshot.Head);
        }

        Recovered();

        try
        {
            await EndAsync(transaction, cancellationToken).ConfigureAwait(false);
            await points.ReachAsync(LogFaultPoints.BetweenPublishAndCursor, cancellationToken).ConfigureAwait(false);
            await WriteCursorAsync(connection, epoch, snapshot.Head, cancellationToken).ConfigureAwait(false);
            await points.ReachAsync(LogFaultPoints.AfterCursorWrite, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            if (!cancellationToken.IsCancellationRequested && !Ended(epoch))
            {
                logger.LogWarning(exception, "The cursor statement of pass {Pass} failed; the loop writes it at its next tick.", pass.Number);
            }
        }

        await channel.PublishAsync(new Wakeup.Wakeup(WakeupKind.Applied, snapshot.Head.Seq), CancellationToken.None).ConfigureAwait(false);
        return PassOutcome.Published;
    }

    internal async Task WriteUnwrittenCursorAsync(long epoch, CancellationToken cancellationToken)
    {
        Cursor head;
        lock (sync)
        {
            if (unwrittenCursor is not { } pending || pending.Epoch != epoch)
            {
                return;
            }

            head = pending.Head;
        }

        await using var connection = passes.CreateConnection();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await WriteCursorAsync(connection, epoch, head, cancellationToken).ConfigureAwait(false);
    }

    private static async Task EndAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        await transaction.DisposeAsync().ConfigureAwait(false);
    }

    private async Task<ReconcileResult> ReconcileAsync(IReconciler reconciler, ReconcileContext context, CancellationToken cancellationToken)
    {
        var result = await reconciler.ReconcileAsync(context, cancellationToken).ConfigureAwait(false);

        await points.ReachAsync(LogFaultPoints.BeforeReconcilerReturns, CancellationToken.None).ConfigureAwait(false);
        return result;
    }

    private async Task<Snapshot> ReadSnapshotAsync(NpgsqlTransaction transaction, Cursor local, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(SnapshotSql, transaction.Connection, transaction);
        command.Parameters.AddWithValue("seq", local.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)local.Token ?? DBNull.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var head = reader.IsDBNull(3) ? Cursor.Empty : new Cursor(reader.GetInt64(3), reader.GetGuid(4));
        return new Snapshot(reader.GetInt64(0), reader.GetString(1), reader.GetBoolean(2), head);
    }

    private static async Task<(long Entries, IReadOnlySet<string> Kept)> ReadFullAsync(
        NpgsqlTransaction transaction, Cursor local, bool keep, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(FullSql, transaction.Connection, transaction);
        command.Parameters.AddWithValue("keep", keep);
        command.Parameters.AddWithValue("seq", local.Seq);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return (reader.GetInt64(0), reader.GetFieldValue<string[]>(1).ToFrozenSet(StringComparer.Ordinal));
    }

    private async Task<(IReadOnlyList<string> Dirty, IReadOnlyDictionary<string, IReadOnlySet<string>?> Hints, long Entries)> ReadDirtyAsync(
        NpgsqlTransaction transaction, Cursor local, bool withoutHints, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(DirtySql, transaction.Connection, transaction);
        command.Parameters.AddWithValue("seq", local.Seq);
        command.Parameters.AddWithValue("limit", Math.Min(hintLimit, int.MaxValue - 1) + 1);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var hints = new Dictionary<string, IReadOnlySet<string>?>(StringComparer.Ordinal);
        long entries = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var type = reader.GetString(0);
            entries += reader.GetInt64(1);
            if (!reconcilers.ContainsKey(type) || reader.GetInt64(2) == 0)
            {
                continue;
            }

            var keys = reader.GetInt64(3);
            var keyless = reader.GetBoolean(4);
            hints[type] = withoutHints || keyless || keys > hintLimit || reader.IsDBNull(5)
                ? null
                : reader.GetFieldValue<string[]>(5).ToFrozenSet(StringComparer.Ordinal);
        }

        return ([.. store.AggregateTypes.Where(hints.ContainsKey)], hints, entries);
    }

    private async Task WriteCursorAsync(NpgsqlConnection connection, long epoch, Cursor head, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(CursorSql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        command.Parameters.AddWithValue("epoch", epoch);
        command.Parameters.AddWithValue("seq", head.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)head.Token ?? DBNull.Value });
        var rows = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        lock (sync)
        {
            if (Ended(epoch))
            {
                return;
            }

            if (unwrittenCursor is { } pending && pending.Epoch == epoch && pending.Head == head)
            {
                unwrittenCursor = null;
            }

            events.Emit(new ModelEvent(LogActions.CursorWrite, new Dictionary<string, object?>
            {
                ["epoch"] = epoch,
                ["seq"] = head.Seq,
                ["token"] = head.Token,
                ["rows"] = rows,
            }));
        }
    }

    private async Task RunLoopAsync(long epoch, CancellationToken cancellationToken)
    {
        var withoutHints = false;
        var now = true;
        var failures = 0;
        while (!cancellationToken.IsCancellationRequested && !Ended(epoch))
        {
            try
            {
                if (!now)
                {
                    await WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                now = false;
                try
                {
                    await WriteUnwrittenCursorAsync(epoch, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not (OutOfMemoryException or OperationCanceledException))
                {
                    logger.LogWarning(exception, "The pending cursor statement of epoch {Epoch} failed; it is retried at the next check.", epoch);
                }

                if (!await PollAsync(cancellationToken).ConfigureAwait(false))
                {
                    continue;
                }

                var outcome = await RunPassAsync(epoch, forceFull: false, withoutHints, cancellationToken).ConfigureAwait(false);
                withoutHints = outcome switch
                {
                    PassOutcome.Stalled => true,
                    PassOutcome.Published or PassOutcome.NothingNew => false,
                    _ => withoutHints,
                };
                failures = outcome switch
                {
                    PassOutcome.Stalled or PassOutcome.Failed => failures + 1,
                    PassOutcome.Published or PassOutcome.NothingNew => 0,
                    _ => failures,
                };
                now = outcome == PassOutcome.Published;
                if (outcome is PassOutcome.Stalled or PassOutcome.Failed)
                {
                    await BackoffAsync(failures, cancellationToken).ConfigureAwait(false);
                    now = true;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogWarning(exception, "The applier's poll of epoch {Epoch} failed; it retries at the next tick.", epoch);
            }
        }
    }

    private async Task BackoffAsync(int failures, CancellationToken cancellationToken)
    {
        var delay = ApplierFailureRules.BeforeStallDeadline(ApplierFailureRules.RetryDelay(failures, pollInterval, retryCap), stall.Left - reconcileTimeout);
        logger.LogInformation("The applier retries in {Delay}, after {Failures} failed or stalled passes in a row.", delay, failures);
        await Task.Delay(delay, time, cancellationToken).ConfigureAwait(false);
        await points.ReachAsync(LogFaultPoints.BeforeBackoffRetry, cancellationToken).ConfigureAwait(false);
    }

    private async Task WaitAsync(CancellationToken cancellationToken)
    {
        var signal = Volatile.Read(ref wakeup);
        if (!signal.Task.IsCompleted)
        {
            using var tick = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var delay = Task.Delay(pollInterval, time, tick.Token);
            await Task.WhenAny(signal.Task, delay).ConfigureAwait(false);
            await tick.CancelAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }

        if (signal.Task.IsCompleted)
        {
            Interlocked.CompareExchange(ref wakeup, NewSignal(), signal);
        }
    }

    private void Wake() => Volatile.Read(ref wakeup).TrySetResult();

    private void FullPassFailed(bool full, string cause)
    {
        if (full)
        {
            lock (sync)
            {
                fullPassFailure = cause;
            }
        }
    }

    private void ReportFailedLoad(long epoch, TimeSpan elapsed)
    {
        string cause;
        lock (sync)
        {
            cause = fullPassFailure ?? "a statement of the load failed";
        }

        const string Sizing = "ReconcileTimeout bounds every reconciler attempt, the full load's included: size it at 2 to 3 times the "
            + "slowest replica's full load of the largest type, plus a pause, or the replica never leaves Loading";
        logger.LogError(
            "The full load of epoch {Epoch} failed after {Elapsed}, with ReconcileTimeout {ReconcileTimeout}: {Cause}. " + Sizing + ". It is retried with backoff.",
            epoch, elapsed, reconcileTimeout, cause);
        gate.Readiness.Raise(new ReadinessReason(
            LoadFailedReason,
            $"The full load of epoch {epoch} failed after {elapsed}, with ReconcileTimeout {reconcileTimeout}: {cause}. {Sizing}. It is retried with backoff."));
    }

    private void Stalled(long epoch, long passStarted, string failingType)
    {
        lock (sync)
        {
            lastPassEntries = 0;
            this.failingType = failingType;
        }

        stall.Stalled(epoch, passStarted);
    }

    private void Recovered()
    {
        lock (sync)
        {
            failingType = null;
        }

        stall.Recovered();
    }

    private bool Ended(long epoch)
    {
        lock (sync)
        {
            return disposed || epoch <= stoppedThrough;
        }
    }

    private void StopThrough(long epoch)
    {
        CancellationTokenSource? ended;
        ApplierPass? pass;
        lock (sync)
        {
            stoppedThrough = Math.Max(stoppedThrough, epoch);
            if (unwrittenCursor is { } pending && pending.Epoch <= stoppedThrough)
            {
                unwrittenCursor = null;
            }

            ended = loop;
            loop = null;
            pass = openPass;
            openPass = null;
        }

        _ = ended?.CancelAsync();
        pass?.Abandon();
    }

    private readonly record struct Snapshot(long Timeline, string SnapshotId, bool CursorValid, Cursor Head);

    private sealed class PassConnection(NpgsqlConnection connection) : IAsyncDisposable
    {
        private int disposed;

        public NpgsqlConnection Connection => connection;

        public ValueTask DisposeAsync() =>
            Interlocked.Exchange(ref disposed, 1) == 0 ? connection.DisposeAsync() : ValueTask.CompletedTask;
    }
}

internal sealed class ApplierStopParticipant(Applier applier) : IFenceParticipant
{
    public FenceStep Step => FenceStep.StopApplier;

    public ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken)
    {
        applier.Stop();
        return ValueTask.CompletedTask;
    }
}
