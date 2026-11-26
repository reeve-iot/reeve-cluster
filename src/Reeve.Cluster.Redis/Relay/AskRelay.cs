using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Redis.Relay;

internal sealed record AskRelayTimings(
    TimeSpan PollInterval,
    TimeSpan WithdrawBudget,
    TimeSpan RecordMargin,
    TimeSpan SubscribeRetry,
    TimeSpan AnswerRetry)
{
    public static AskRelayTimings Default { get; } = new(
        TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(200));
}

internal enum AskSubscriptionState
{
    Holding,

    NotSubscribed,

    Lost,
}

internal sealed class AskRelay : IAskTransport, IAsyncDisposable
{
    private readonly IAskWire wire;
    private readonly IAskInbound inbound;
    private readonly AskCells cells;
    private readonly AskRelayTimings timings;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly Guid processId;
    private readonly ConcurrentDictionary<Guid, ReplySignal> waiting = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly TaskCompletionSource subscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task? subscribing;
    private int disposed;

    public AskRelay(
        IAskWire wire,
        IAskInbound inbound,
        MemberIdentity identity,
        IOptions<ClusterOptions> options,
        AskRelayTimings timings,
        TimeProvider time,
        ILogger<AskRelay> logger)
    {
        ArgumentNullException.ThrowIfNull(wire);
        ArgumentNullException.ThrowIfNull(inbound);
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timings);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);
        this.wire = wire;
        this.inbound = inbound;
        this.timings = timings;
        this.time = time;
        this.logger = logger;
        cells = new AskCells(wire, timings.RecordMargin, logger);
        processId = identity.ProcessId;
        replicaId = options.Value.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    }

    public Task Subscribed => subscribed.Task;

    public AskSubscriptionState SubscriptionState =>
        !subscribed.Task.IsCompleted
            ? AskSubscriptionState.NotSubscribed
            : wire.IsSubscribed(AskAddress.RequestChannel(replicaId, processId)) && wire.IsSubscribed(AskAddress.ReplyChannel(processId))
                ? AskSubscriptionState.Holding
                : AskSubscriptionState.Lost;

    public bool Reachable => wire.IsSubscribed(AskAddress.RequestChannel(replicaId, processId));

    public void Start()
    {
        if (subscribing is not null)
        {
            return;
        }

        subscribing = Flowless(() => SubscribeAsync(stopping.Token));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);
        if (subscribing is not null)
        {
            await subscribing.ConfigureAwait(false);
        }

        wire.Unsubscribe(AskAddress.RequestChannel(replicaId, processId));
        wire.Unsubscribe(AskAddress.ReplyChannel(processId));
        stopping.Dispose();
    }

    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, CancellationToken cancellationToken) =>
        DeliverAsync(target, delivery, timeout, takeTimeout: null, cancellationToken);

    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken) =>
        DeliverAsync(target, delivery, timeout, (TimeSpan?)takeTimeout, cancellationToken);

    private async Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, TimeSpan? takeTimeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        var id = Guid.NewGuid();
        var window = AskRelayDecisions.AnswerWindow(timeout, timings.WithdrawBudget);
        var message = new AskRequestMessage(id, processId, window, delivery).Encode();
        using var signal = new ReplySignal(waiting, id);

        using var callEnds = new CancellationTokenSource(timeout, time);
        using var windowEnds = new CancellationTokenSource(window, time);
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, windowEnds.Token);
        using var takeEnds = takeTimeout is { } take ? new CancellationTokenSource(take, time) : null;
        try
        {
            var connectionEvents = wire.ConnectionEvents;
            var submit = await cells.SubmitAsync(
                id, AskRelayDecisions.RecordTtl(timeout, timings.RecordMargin), AskAddress.RequestChannel(target.ReplicaId, target.ProcessId), message, wait.Token).ConfigureAwait(false);
            var step = AskRelayDecisions.AfterSubmit(submit);
            if (step.Action == CallerAction.End)
            {
                return step.Outcome!.Value;
            }

            if (step.Action == CallerAction.Wait)
            {
                var untilTaken = takeEnds;
                while (!wait.IsCancellationRequested)
                {
                    if (untilTaken is { IsCancellationRequested: true })
                    {
                        untilTaken = null;
                        using var takeBudget = new CancellationTokenSource(timings.WithdrawBudget, time);
                        using var takeWithdrawEnds = CancellationTokenSource.CreateLinkedTokenSource(wait.Token, takeBudget.Token);
                        var early = await cells.WithdrawAsync(id, takeWithdrawEnds.Token).ConfigureAwait(false);
                        if (AskRelayDecisions.AtTakeBound(early, wire.ConnectionEvents != connectionEvents) is { Action: CallerAction.End } settled)
                        {
                            return settled.Outcome!.Value;
                        }

                        continue;
                    }

                    using var woken = untilTaken is null ? null : CancellationTokenSource.CreateLinkedTokenSource(wait.Token, untilTaken.Token);
                    await signal.WaitAsync(timings.PollInterval, time, woken?.Token ?? wait.Token).ConfigureAwait(false);
                    var (cell, read) = await cells.ReadAsync(id, wait.Token).ConfigureAwait(false);
                    if (read && AskRelayDecisions.OnObserved(cell) is { Action: CallerAction.End } ended)
                    {
                        return ended.Outcome!.Value;
                    }
                }
            }

            using var budget = new CancellationTokenSource(timings.WithdrawBudget, time);
            using var withdrawEnds = CancellationTokenSource.CreateLinkedTokenSource(callEnds.Token, budget.Token);
            var withdrawal = await cells.WithdrawAsync(id, withdrawEnds.Token).ConfigureAwait(false);
            return AskRelayDecisions.AfterWithdrawal(withdrawal, wire.ConnectionEvents != connectionEvents);
        }
        finally
        {
            cells.Forget(id);
        }
    }

    private async Task SubscribeAsync(CancellationToken stop)
    {
        var channels = new (string Name, Action<byte[]> Handler)[]
        {
            (AskAddress.RequestChannel(replicaId, processId), OnRequest),
            (AskAddress.ReplyChannel(processId), OnReply),
        };
        var held = new HashSet<string>();
        var warned = false;
        while (!stop.IsCancellationRequested)
        {
            Exception? failure = null;
            foreach (var (name, handler) in channels)
            {
                if (held.Contains(name))
                {
                    continue;
                }

                try
                {
                    await wire.SubscribeAsync(name, handler, stop).ConfigureAwait(false);
                    held.Add(name);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    failure = exception;
                    wire.Unsubscribe(name);
                }
            }

            if (failure is null)
            {
                subscribed.TrySetResult();
                if (warned)
                {
                    logger.LogInformation("The Ask relay's subscriptions hold now.");
                }

                return;
            }

            if (!warned)
            {
                warned = true;
                logger.LogWarning(failure, "Subscribing the Ask relay failed; requests to this process end NotTaken until it holds. It is tried again every {Seconds} s.", timings.SubscribeRetry.TotalSeconds);
            }

            try
            {
                await Task.Delay(timings.SubscribeRetry, time, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private void OnReply(byte[] message)
    {
        if (message.Length == 16 && waiting.TryGetValue(new Guid(message), out var signal))
        {
            signal.Set();
        }
    }

    private void OnRequest(byte[] bytes)
    {
        if (AskRequestMessage.TryDecode(bytes) is not { } request)
        {
            logger.LogDebug("Ignored a malformed Ask request message.");
            return;
        }

        _ = Flowless(() => ReceiveAsync(request));
    }

    private async Task ReceiveAsync(AskRequestMessage request)
    {
        try
        {
            using var answerBy = new CancellationTokenSource(request.Window, time);
            var reservation = await inbound.ReserveAsync(request.Delivery, answerBy.Token).ConfigureAwait(false);
            if (!reservation.IsReserved)
            {
                await cells.DeclineAsync(request.RequestId, reservation.Reason ?? DeclineReason.Absent, answerBy.Token).ConfigureAwait(false);
                await NotifyAsync(request).ConfigureAwait(false);
                return;
            }

            var taken = await cells.TakeAsync(request.RequestId, answerBy.Token).ConfigureAwait(false);
            if (!AskRelayDecisions.StartsHandler(taken))
            {
                logger.LogDebug("An Ask request was not taken here ({End}); no handler started.", taken);
                return;
            }

            var answer = await reservation.Start().ConfigureAwait(false);
            if (answer.Kind != AskAnswerKind.Unanswered)
            {
                await WriteAnswerAsync(request, answer, answerBy.Token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "Receiving an Ask request failed.");
        }
    }

    private async Task WriteAnswerAsync(AskRequestMessage request, AskAnswer answer, CancellationToken answerBy)
    {
        while (true)
        {
            var end = await cells.AnswerAsync(request.RequestId, answer, answerBy.IsCancellationRequested ? CancellationToken.None : answerBy).ConfigureAwait(false);
            if (AskRelayDecisions.AfterAnswerWrite(end, answerBy.IsCancellationRequested) == AnswerStep.Done)
            {
                if (end == AnswerEnd.Written)
                {
                    await NotifyAsync(request).ConfigureAwait(false);
                }

                return;
            }

            try
            {
                await Task.Delay(timings.AnswerRetry, time, answerBy).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task NotifyAsync(AskRequestMessage request)
    {
        try
        {
            using var budget = new CancellationTokenSource(timings.WithdrawBudget, time);
            await wire.PublishAsync(AskAddress.ReplyChannel(request.CallerProcess), request.RequestId.ToByteArray(), budget.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Telling the caller of an Ask request that its record changed failed; its poll finds it.");
        }
    }

    private static Task Flowless(Func<Task> work)
    {
        if (ExecutionContext.IsFlowSuppressed())
        {
            return Task.Run(work);
        }

        using (ExecutionContext.SuppressFlow())
        {
            return Task.Run(work);
        }
    }

    private sealed class ReplySignal : IDisposable
    {
        private readonly ConcurrentDictionary<Guid, ReplySignal> owner;
        private readonly Guid id;
        private readonly object gate = new();
        private TaskCompletionSource armed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private bool set;

        public ReplySignal(ConcurrentDictionary<Guid, ReplySignal> owner, Guid id)
        {
            this.owner = owner;
            this.id = id;
            owner[id] = this;
        }

        public void Set()
        {
            lock (gate)
            {
                set = true;
                armed.TrySetResult();
            }
        }

        public async Task WaitAsync(TimeSpan interval, TimeProvider time, CancellationToken cancellationToken)
        {
            Task pending;
            lock (gate)
            {
                if (set)
                {
                    set = false;
                    armed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    return;
                }

                pending = armed.Task;
            }

            try
            {
                await pending.WaitAsync(interval, time, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is TimeoutException or OperationCanceledException)
            {
            }

            lock (gate)
            {
                set = false;
                armed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public void Dispose() => owner.TryRemove(id, out _);
    }
}
