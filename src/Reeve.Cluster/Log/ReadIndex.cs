using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

#pragma warning disable CA2007

namespace Reeve.Cluster.Log;

internal enum ReadIndexOutcome
{
    Pinned,

    Behind,

    NotServing,
}

internal readonly record struct ReadIndexResult(ReadIndexOutcome Outcome, IGeneration? Generation, ServingCheckResult? Refusal);

internal readonly record struct ReadIndexRead(long Head, bool CursorValid);

internal interface IReadIndexStore
{
    Task<ReadIndexRead> ReadAsync(Cursor cursor, CancellationToken cancellationToken);
}

internal sealed class ReadIndexDataSource : IAsyncDisposable
{
    public const int MaxPoolSize = 16;

    public ReadIndexDataSource(IOptions<ClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Source = new ClusterDataSource(options.Value, static builder =>
        {
            builder.ApplicationName = "reeve-cluster read index";
            builder.MaxPoolSize = Math.Min(MaxPoolSize, builder.MaxPoolSize);
        });
    }

    public ClusterDataSource Source { get; }

    public ValueTask DisposeAsync() => Source.DisposeAsync();
}

internal sealed class ReadIndexStore(ClusterDataSource data) : IReadIndexStore
{
    internal ClusterDataSource Data => data;

    internal const string ReadSql = """
        SELECT coalesce((SELECT max(seq) FROM reeve_cluster.change_log), 0), reeve_cluster.cursor_valid(@seq, @token)
        """;

    public async Task<ReadIndexRead> ReadAsync(Cursor cursor, CancellationToken cancellationToken)
    {
        await using var connection = await data.DataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(ReadSql, connection);
        command.Parameters.AddWithValue("seq", cursor.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)cursor.Token ?? DBNull.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new ReadIndexRead(reader.GetInt64(0), reader.GetBoolean(1));
    }
}

internal sealed class ReadIndex : IDisposable
{
    private readonly IPhaseGate gate;
    private readonly IGenerationSource generations;
    private readonly IReadIndexStore store;
    private readonly TimeProvider time;
    private readonly IFaultPoints points;
    private readonly IModelEventSink events;
    private readonly ILogger logger;
    private readonly TimeSpan timeout;
    private readonly TimeSpan pollInterval;
    private readonly IDisposable applied;

    private TaskCompletionSource appliedSignal = NewSignal();

    private int failing;

    private long behind;

    public ReadIndex(
        IPhaseGate gate,
        IGenerationSource generations,
        IReadIndexStore store,
        IWakeupChannel channel,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        IFaultPoints points,
        IModelEventSink events,
        ILogger<ReadIndex> logger)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(generations);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(points);
        ArgumentNullException.ThrowIfNull(events);
        ArgumentNullException.ThrowIfNull(logger);
        this.gate = gate;
        this.generations = generations;
        this.store = store;
        this.time = time;
        this.points = points;
        this.events = events;
        this.logger = logger;
        timeout = options.Value.ReadIndexTimeout;
        pollInterval = options.Value.ApplyPollInterval;

        applied = channel.Subscribe(WakeupKind.Applied, _ => Volatile.Read(ref appliedSignal).TrySetResult());
    }

    public async Task<ReadIndexResult> EnterAsync(IClusterScopeWriter scope, bool wait, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (scope.PinnedGeneration is not null)
        {
            throw new InvalidOperationException("The scope has pinned a generation already: the read index runs once per scope.");
        }

        if (!wait)
        {
            var current = scope.PinCurrent(generations);
            await points.ReachAsync(LogFaultPoints.BetweenPinAndRead, cancellationToken).ConfigureAwait(false);
            return new ReadIndexResult(ReadIndexOutcome.Pinned, current, null);
        }

        using var call = new Call(time, timeout, cancellationToken);
        var request = (scope as ClusterScope)?.Id;
        long? head = null;

        IGeneration? found = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (call.Expired)
            {
                if (head is { } h)
                {
                    Emit(LogActions.QTimeout, request, (ObservationKeys.Head, h));
                }

                if (Interlocked.Increment(ref behind) == 1)
                {
                    logger.LogWarning(
                        "The read index did not catch up with head {Head} within ReadIndexTimeout {Timeout}: requests are answered replica_behind. Later ones are logged at Debug until a request that waits for the read index is admitted again.",
                        head, timeout);
                }
                else
                {
                    logger.LogDebug("The read index did not catch up with head {Head} within ReadIndexTimeout {Timeout}.", head, timeout);
                }

                return new ReadIndexResult(ReadIndexOutcome.Behind, null, null);
            }

            var check = gate.CheckServing();
            if (!check.IsServing)
            {
                return new ReadIndexResult(ReadIndexOutcome.NotServing, null, check);
            }

            var notice = NextApplied();
            var current = generations.Current;
            if (current is not null && !ReferenceEquals(current, found) && (head is null || current.Head >= head))
            {
                if (await TryReadAsync(current, call).ConfigureAwait(false) is not { } read)
                {
                    if (!call.Over)
                    {
                        await call.WaitAsync(notice, pollInterval).ConfigureAwait(false);
                    }

                    continue;
                }

                if (head is null)
                {
                    var started = gate.CheckServing();
                    if (!started.IsServing)
                    {
                        return new ReadIndexResult(ReadIndexOutcome.NotServing, null, started);
                    }

                    head = read.Head;
                    Emit(LogActions.QStart, request, (ObservationKeys.Head, read.Head));
                }

                if (!read.CursorValid || current.Head < head)
                {
                    found = current;
                }
                else if (await PinAsync(scope, current, request, cancellationToken).ConfigureAwait(false) is { } answer)
                {
                    return answer;
                }
                else
                {
                    continue;
                }
            }

            await call.WaitAsync(notice, pollInterval).ConfigureAwait(false);
        }
    }

    public void Dispose() => applied.Dispose();

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task<ReadIndexResult?> PinAsync(IClusterScopeWriter scope, IGeneration generation, string? request, CancellationToken cancellationToken)
    {
        await points.ReachAsync(LogFaultPoints.BetweenWaitAndPin, cancellationToken).ConfigureAwait(false);
        var check = gate.CheckServing();
        if (!check.IsServing)
        {
            return new ReadIndexResult(ReadIndexOutcome.NotServing, null, check);
        }

        if (!ReferenceEquals(generations.Current, generation))
        {
            return null;
        }

        scope.Pin(generation);
        var id = (generation as Generation)?.Id;
        Emit(LogActions.QPin, request, (ObservationKeys.Generation, id), (ObservationKeys.Head, generation.Head));
        await points.ReachAsync(LogFaultPoints.BetweenPinAndRead, cancellationToken).ConfigureAwait(false);
        Emit(LogActions.QRead, request, (ObservationKeys.Generation, id), (ObservationKeys.Head, generation.Head));
        if (Interlocked.Exchange(ref behind, 0) is var answered and > 0)
        {
            logger.LogInformation("The read index caught up again at head {Head}, after {Behind} requests answered replica_behind.", generation.Head, answered);
        }

        return new ReadIndexResult(ReadIndexOutcome.Pinned, generation, null);
    }

    private async Task<ReadIndexRead?> TryReadAsync(IGeneration generation, Call call)
    {
        Task<ReadIndexRead> reading;
        try
        {
            reading = store.ReadAsync(new Cursor(generation.Head, generation.HeadToken), call.ReadToken);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            reading = Task.FromException<ReadIndexRead>(exception);
        }

        await Task.WhenAny(reading, call.Ended).ConfigureAwait(false);
        if (call.Over || !reading.IsCompleted)
        {
            Observe(reading);
            return null;
        }

        if (reading.IsCompletedSuccessfully)
        {
            Volatile.Write(ref failing, 0);
            return reading.Result;
        }

        var level = Interlocked.Exchange(ref failing, 1) == 0 ? LogLevel.Warning : LogLevel.Debug;
        logger.Log(level, reading.Exception?.InnerException, "A read-index read failed; it is retried at the next wake-up.");
        return null;
    }

    private void Observe(Task reading) =>
        _ = reading.ContinueWith(
            t => logger.LogDebug(t.Exception?.InnerException, "An abandoned read-index read failed."),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private Task NextApplied()
    {
        var signal = Volatile.Read(ref appliedSignal);
        if (signal.Task.IsCompleted)
        {
            Interlocked.CompareExchange(ref appliedSignal, NewSignal(), signal);
            signal = Volatile.Read(ref appliedSignal);
        }

        return signal.Task;
    }

    private void Emit(string action, string? request, params (string Key, object? Value)[] state)
    {
        var payload = new Dictionary<string, object?> { [ObservationKeys.Request] = request };
        foreach (var (key, value) in state)
        {
            payload[key] = value;
        }

        events.Emit(new ModelEvent(action, payload));
    }

    private sealed class Call : IDisposable
    {
        private readonly TaskCompletionSource expired = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource ended = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource read = new();
        private readonly TimeProvider time;
        private readonly ITimer timer;
        private readonly CancellationTokenRegistration callerCancel;

        public Call(TimeProvider time, TimeSpan bound, CancellationToken cancellationToken)
        {
            this.time = time;
            timer = time.CreateTimer(_ => End(expired), null, bound, Timeout.InfiniteTimeSpan);
            callerCancel = cancellationToken.Register(() => End(null));
        }

        public bool Expired => expired.Task.IsCompleted;

        public bool Over => ended.Task.IsCompleted;

        public Task Ended => ended.Task;

        public CancellationToken ReadToken => read.Token;

        public async Task WaitAsync(Task notice, TimeSpan poll)
        {
            using var tick = new CancellationTokenSource();
            var delay = Task.Delay(poll, time, tick.Token);
            await Task.WhenAny(notice, delay, ended.Task).ConfigureAwait(false);
            await tick.CancelAsync().ConfigureAwait(false);
        }

        public void Dispose()
        {
            timer.Dispose();
            callerCancel.Dispose();
        }

        private void End(TaskCompletionSource? cause)
        {
            cause?.TrySetResult();
            if (ended.TrySetResult())
            {
                _ = read.CancelAsync();
            }
        }
    }
}
