using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Log;

internal static class AckWaitEnd
{
    public static UnconfirmedReason? On(PhaseEvent phaseEvent) => phaseEvent switch
    {
        { Kind: PhaseEventKind.DrainStep4 } => UnconfirmedReason.Draining,
        { Kind: PhaseEventKind.Transition, To: ReplicaPhase.Serving or ReplicaPhase.Draining } => null,
        { Kind: PhaseEventKind.Transition } => UnconfirmedReason.Fenced,
        _ => null,
    };
}

internal sealed record AckWaitSetRead(DateTimeOffset Now, bool LocalValid, IReadOnlyList<(string ReplicaId, AckMember Member)> Members);

internal sealed class ChangeAck : IChangeAck, IDisposable
{
    private const string WaitSetSql = """
        WITH t AS (SELECT clock_timestamp() AS now, reeve_cluster.cursor_valid($1, $2) AS local_valid)
        SELECT t.now, t.local_valid, m.replica_id, m.state, m.lease_expires_at, m.activated_at_seq, m.applied_seq,
               reeve_cluster.cursor_valid(m.applied_seq, m.applied_token)
        FROM t LEFT JOIN reeve_cluster.member m ON true
        """;

    private static long lastWaitId;

    private readonly IClusterScopeSource scopes;
    private readonly CommitRefusal refusal;
    private readonly IPhaseGate gate;
    private readonly IGenerationSource generations;
    private readonly ClusterDataSource data;
    private readonly AckRecorder recorder;
    private readonly IWakeupChannel channel;
    private readonly TimeProvider time;
    private readonly IFaultPoints faultPoints;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly TimeSpan ackTimeout;
    private readonly TimeSpan pollInterval;
    private readonly IDisposable applied;
    private TaskCompletionSource appliedSignal = NewSignal();

    internal ClusterDataSource Data => data;

    public ChangeAck(
        IClusterScopeSource scopes,
        CommitRefusal refusal,
        IPhaseGate gate,
        IGenerationSource generations,
        ClusterDataSource data,
        AckRecorder recorder,
        IWakeupChannel channel,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints faultPoints,
        IModelEventSink events,
        ILogger<ChangeAck> logger)
    {
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentNullException.ThrowIfNull(refusal);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(generations);
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(recorder);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(faultPoints);
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(logger);

        this.scopes = scopes;
        this.refusal = refusal;
        this.gate = gate;
        this.generations = generations;
        this.data = data;
        this.recorder = recorder;
        this.channel = channel;
        this.time = time;
        this.faultPoints = faultPoints;
        this.events = events;
        this.logger = logger;
        ackTimeout = options.Value.AckTimeout;
        pollInterval = options.Value.AckPollInterval;

        applied = channel.Subscribe(WakeupKind.Applied, _ => Interlocked.Exchange(ref appliedSignal, NewSignal()).TrySetResult());
    }

    public IReadOnlyList<long> Seqs => scopes.CurrentWriter?.Seqs ?? [];

    public void AcknowledgeAtCommit()
    {
        if (scopes.CurrentWriter is not { } scope)
        {
            return;
        }

        scope.OptOut();
        events.Emit(new ModelEvent(LogActions.AckAtCommit, new Dictionary<string, object?>
        {
            [ObservationKeys.Request] = (scope as ClusterScope)?.Id,
        }));
    }

    public async Task<AckOutcome> WaitAppliedAsync(long seq, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(seq, 1);
        cancellationToken.ThrowIfCancellationRequested();

        var scope = scopes.CurrentWriter;
        var request = (scope as ClusterScope)?.Id ?? "wait-" + Interlocked.Increment(ref lastWaitId).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var caller = new AckCaller(
            scope?.Kind,
            scope?.AcknowledgedAtCommit ?? false,
            scope?.Seqs.Contains(seq) ?? false,
            scope?.CommitRefused ?? false,
            refusal.RanThroughStep2(scope));

        await channel.PublishAsync(new Wakeup.Wakeup(WakeupKind.Appended, seq), CancellationToken.None).ConfigureAwait(false);

        var wait = new Wait(events, request, seq);
        using var phaseEvents = gate.Subscribe(e =>
        {
            if (AckWaitEnd.On(e) is { } reason)
            {
                wait.TryAnswer(new AckOutcome.Unconfirmed(reason));
            }
        });

        if (AckAnswerPolicy.Decide(gate.Phase, caller).Outcome is { } atOnce)
        {
            wait.TryAnswer(atOnce);
            return atOnce;
        }

        if (gate.DrainStep4Passed)
        {
            wait.TryAnswer(new AckOutcome.Unconfirmed(UnconfirmedReason.Draining));
            return await wait.Answer.Task.ConfigureAwait(false);
        }

        var deadline = time.GetTimestamp() + (long)(ackTimeout.Ticks * (decimal)time.TimestampFrequency / TimeSpan.TicksPerSecond);
        using var timer = time.CreateTimer(_ => wait.TryAnswer(RecordTimeout.TimedOut), null, ackTimeout, Timeout.InfiniteTimeSpan);
        var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var loop = WaitLoopAsync(wait, deadline, stop.Token);
        try
        {
            return await wait.Answer.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            var cancelling = stop.CancelAsync();
            _ = Task.WhenAll(loop, cancelling).ContinueWith(
                static (ended, source) =>
                {
                    _ = ended.Exception;
                    ((CancellationTokenSource)source!).Dispose();
                },
                stop,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    public void Dispose() => applied.Dispose();

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task WaitLoopAsync(Wait wait, long deadline, CancellationToken stop)
    {
        try
        {
            while (!wait.Answered)
            {
                var notice = Volatile.Read(ref appliedSignal).Task;
                try
                {
                    var (bound, waitSet) = await CountAsync(wait.Seq, stop).ConfigureAwait(false);
                    wait.Begin(waitSet);
                    if (bound is { } x)
                    {
                        await faultPoints.ReachAsync(LogFaultPoints.AckBetweenCountAndRecord, stop).ConfigureAwait(false);
                        if (!wait.Answered)
                        {
                            await RecordAsync(wait, x, waitSet, deadline, stop).ConfigureAwait(false);
                        }

                        return;
                    }
                }
                catch (Exception exception) when (exception is not OutOfMemoryException && !stop.IsCancellationRequested)
                {
                    logger.LogWarning(exception, "A round of the wait for seq {Seq} failed; retrying at the next wake-up.", wait.Seq);
                }

                using var round = CancellationTokenSource.CreateLinkedTokenSource(stop);
                await Task.WhenAny(notice, Task.Delay(pollInterval, time, round.Token)).ConfigureAwait(false);
                await round.CancelAsync().ConfigureAwait(false);
                stop.ThrowIfCancellationRequested();
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && stop.IsCancellationRequested)
        {
        }
    }

    private async Task<(long? Bound, IReadOnlyList<string> WaitSet)> CountAsync(long seq, CancellationToken stop)
    {
        await faultPoints.ReachAsync(LogFaultPoints.AckBeforeWaitSetRead, stop).ConfigureAwait(false);

        var local = generations.Current is { } generation ? new Cursor(generation.Head, generation.HeadToken) : Cursor.Empty;
        var read = await ReadWaitSetAsync(local, stop).ConfigureAwait(false);
        var waitSet = read.Members.Where(m => AckWaitSet.Includes(m.Member, seq, read.Now)).Select(m => m.ReplicaId).ToList();
        var bound = AckWaitSet.ReadyBound(seq, new AckCursor(local.Seq, read.LocalValid), read.Members.Select(m => m.Member), read.Now);
        return (bound, waitSet);
    }

    private async Task RecordAsync(Wait wait, long bound, IReadOnlyList<string> waitSet, long deadline, CancellationToken stop)
    {
        IReadOnlyList<string> skipped;
        try
        {
            skipped = await recorder.RecordAsync(wait.Seq, bound, deadline, stop).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            wait.TryAnswer(RecordTimeout.TimedOut);
            return;
        }

        wait.TryAnswer(new AckOutcome.Acknowledged(skipped), bound, waitSet);
    }

    private async Task<AckWaitSetRead> ReadWaitSetAsync(Cursor local, CancellationToken cancellationToken)
    {
        var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (connection.ConfigureAwait(false))
        {
            var command = new NpgsqlCommand(WaitSetSql, connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<long> { TypedValue = local.Seq },
                    new NpgsqlParameter { Value = (object?)local.Token ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Uuid },
                },
            };
            await using (command.ConfigureAwait(false))
            {
                var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await using (reader.ConfigureAwait(false))
                {
                    var now = DateTimeOffset.MinValue;
                    var localValid = false;
                    var members = new List<(string, AckMember)>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        now = reader.GetFieldValue<DateTimeOffset>(0);
                        localValid = reader.GetBoolean(1);
                        if (reader.IsDBNull(2))
                        {
                            continue;
                        }

                        members.Add((reader.GetString(2), new AckMember(
                            Enum.Parse<MemberRowState>(reader.GetString(3), ignoreCase: true),
                            reader.GetFieldValue<DateTimeOffset>(4),
                            reader.IsDBNull(5) ? long.MaxValue : reader.GetInt64(5),
                            new AckCursor(reader.GetInt64(6), reader.GetBoolean(7)))));
                    }

                    return new AckWaitSetRead(now, localValid, members);
                }
            }
        }
    }

    private sealed class Wait(IModelEventSink events, string request, long seq)
    {
        private readonly object sync = new();
        private bool begun;

        public long Seq => seq;

        public TaskCompletionSource<AckOutcome> Answer { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Answered => Answer.Task.IsCompleted;

        public void Begin(IReadOnlyList<string> waitSet)
        {
            lock (sync)
            {
                if (!begun && !Answered)
                {
                    EmitBegin(waitSet, waiting: true);
                }
            }
        }

        public bool TryAnswer(AckOutcome outcome, long? bound = null, IReadOnlyList<string>? waitSet = null)
        {
            lock (sync)
            {
                if (Answered)
                {
                    return false;
                }

                if (!begun)
                {
                    EmitBegin([], waiting: false);
                }

                switch (outcome)
                {
                    case AckOutcome.Acknowledged acknowledged:
                        events.Emit(new ModelEvent(LogActions.AckDone, new Dictionary<string, object?>
                        {
                            [ObservationKeys.Request] = request,
                            [ObservationKeys.AwaitedSeq] = seq,
                            [ObservationKeys.WaitSet] = waitSet ?? [],
                            [ObservationKeys.Bound] = bound,
                            [ObservationKeys.Skipped] = acknowledged.Skipped,
                        }));
                        break;
                    case AckOutcome.Unconfirmed { Reason: UnconfirmedReason.Timeout }:
                        events.Emit(new ModelEvent(LogActions.AckTimeout, new Dictionary<string, object?>
                        {
                            [ObservationKeys.Request] = request,
                            [ObservationKeys.AwaitedSeq] = seq,
                        }));
                        break;
                }

                Answer.SetResult(outcome);
                return true;
            }
        }

        private void EmitBegin(IReadOnlyList<string> waitSet, bool waiting)
        {
            begun = true;
            events.Emit(new ModelEvent(LogActions.AckBegin, new Dictionary<string, object?>
            {
                [ObservationKeys.Request] = request,
                [ObservationKeys.AwaitedSeq] = seq,
                [ObservationKeys.WaitSet] = waitSet,
                [ObservationKeys.Waiting] = waiting,
            }));
        }
    }
}
