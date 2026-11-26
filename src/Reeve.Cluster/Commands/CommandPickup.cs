using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Commands;

internal sealed class CommandHandlers(IReadOnlyDictionary<string, (IDurableCommandHandler Handler, InDoubtPolicy InDoubt)> byKind)
{
    public IReadOnlyCollection<string> Kinds => byKind.Keys.ToArray();

    public bool Has(string kind) => byKind.ContainsKey(kind);

    public IDurableCommandHandler? For(string kind) => byKind.TryGetValue(kind, out var found) ? found.Handler : null;

    public InDoubtPolicy? PolicyOf(string kind) => byKind.TryGetValue(kind, out var found) ? found.InDoubt : null;
}

internal sealed class CommandPickup : IHostedService, IDisposable
{
    private readonly ICommandRows rows;
    private readonly ICommandTenures tenures;
    private readonly CommandHandlers handlers;
    private readonly IClusterScopeSource scopes;
    private readonly IFaultPoints points;
    private readonly IOptions<ClusterOptions> optionsAccessor;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<(string Kind, string Key), KeyState> keys = new();
    private readonly IDisposable nudges;
    private readonly IWakeupChannel wakeups;
    private readonly CancellationTokenSource stopping = new();
    private readonly CancellationTokenRegistration applicationStopping;
    private readonly CancellationToken applicationStoppingToken;
    private TaskCompletionSource wake = NewWake();
    private Task? running;
    private int polls;
    private int disposed;

    public CommandPickup(
        ICommandRows rows,
        ICommandTenures tenures,
        CommandHandlers handlers,
        IEnumerable<UnitKind> registered,
        IClusterScopeSource scopes,
        IFaultPoints points,
        IWakeupChannel wakeups,
        IHostApplicationLifetime lifetime,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<CommandPickup> logger)
    {
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(lifetime);
        ArgumentNullException.ThrowIfNull(wakeups);
        ArgumentNullException.ThrowIfNull(options);
        this.rows = rows;
        this.tenures = tenures;
        this.handlers = handlers;
        this.scopes = scopes;
        this.points = points;
        this.wakeups = wakeups;
        optionsAccessor = options;
        this.options = options.Value;
        this.time = time;
        this.logger = logger;

        var kinds = registered.ToDictionary(k => k.Kind, StringComparer.Ordinal);
        foreach (var kind in kinds.Values.Where(k => k.Options.Convergent && handlers.Has(k.Kind)))
        {
            throw new InvalidOperationException($"Kind {kind.Kind} is convergent and has a durable command handler: a command is taken under the key's token, which a convergent unit does not get.");
        }

        foreach (var kind in handlers.Kinds.Where(k => !kinds.ContainsKey(k)))
        {
            throw new InvalidOperationException($"A durable command handler is registered for the kind '{kind}', which is not registered as a unit or a keyed resource.");
        }

        applicationStoppingToken = lifetime.ApplicationStopping;
        applicationStopping = lifetime.ApplicationStopping.Register(static state => ((CommandPickup)state!).stopping.Cancel(), this);
        tenures.TenureStarted += Nudge;
        nudges = wakeups.Subscribe(WakeupKind.CommandNudge, w =>
        {
            if (w.Target is null || handlers.Has(w.Target))
            {
                Nudge();
            }
        });
    }

    public void Nudge() => Volatile.Read(ref wake).TrySetResult();

    internal int Polls => Volatile.Read(ref polls);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (handlers.Kinds.Count > 0)
        {
            Volatile.Write(ref running, Task.Run(() => PollAsync(stopping.Token), CancellationToken.None));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);
        if (Volatile.Read(ref running) is { } loop)
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        tenures.TenureStarted -= Nudge;
        nudges.Dispose();
        applicationStopping.Dispose();
        stopping.Cancel();
    }

    internal async Task PassAsync(CancellationToken cancellationToken)
    {
        var kinds = handlers.Kinds;
        var running = tenures.RunningOf(kinds);
        Forget(running);
        if (running.Count == 0)
        {
            return;
        }

        IReadOnlyList<(string Kind, string Key)> active;
        try
        {
            using var bound = Bound(cancellationToken);
            active = await rows.ListActiveKeysAsync(kinds, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The command poll got no answer; the next poll reads again.");
            return;
        }

        var wanted = active.ToHashSet();
        foreach (var tenure in running)
        {
            if (!wanted.Contains((tenure.Kind, tenure.Key)))
            {
                continue;
            }

            var state = keys.GetOrAdd((tenure.Kind, tenure.Key), static _ => new KeyState());
            lock (state)
            {
                if (state.Step is { IsCompleted: false } || state.Handling is { IsCompleted: false })
                {
                    continue;
                }

                using (ExecutionContext.SuppressFlow())
                {
                    state.Step = Task.Run(() => StepAsync(tenure, state, cancellationToken), CancellationToken.None);
                }
            }
        }
    }

    internal Task WhenStepsEndAsync() =>
        Task.WhenAll(keys.Values.Select(k => { lock (k) { return k.Step ?? Task.CompletedTask; } }));

    private void Forget(IReadOnlyList<CommandTenure> running)
    {
        var live = running.Select(t => (t.Kind, t.Key)).ToHashSet();
        foreach (var (key, state) in keys)
        {
            lock (state)
            {
                if (!live.Contains(key) && state.Step is null or { IsCompleted: true } && state.Handling is null or { IsCompleted: true })
                {
                    keys.TryRemove(key, out _);
                }
            }
        }
    }

    private async Task StepAsync(CommandTenure tenure, KeyState state, CancellationToken host)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(host, tenure.Stopped);
        var token = linked.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                IReadOnlyList<ActiveCommandRow> active;
                using (var bound = Bound(token))
                {
                    active = await rows.ReadActiveAsync(tenure.Kind, tenure.Key, bound.Token).ConfigureAwait(false);
                }

                var step = PickupDecisions.Next([.. active.Select(r => r.Active)], tenure.Token, state.Flight);
                bool again;
                switch (step.Action)
                {
                    case PickupAction.Take:
                        again = await TakeAndRunAsync(tenure, state, step.Id, host, token).ConfigureAwait(false);
                        break;
                    case PickupAction.Run:
                        again = await RunAsync(tenure, state, active.First(r => r.Id == step.Id), host, token).ConfigureAwait(false);
                        break;
                    case PickupAction.CompleteAgain:
                        again = await CompleteAsync(tenure, state, step.Id, token).ConfigureAwait(false);
                        break;
                    case PickupAction.ClassifyOlder:
                        again = await ClassifyAsync(tenure, state, active.First(r => r.Id == step.Id), host, token).ConfigureAwait(false);
                        break;
                    default:
                        again = false;
                        break;
                }

                if (!again)
                {
                    return;
                }
            }
        }
        catch (OwnershipLostException exception)
        {
            logger.LogInformation(exception, "{Kind}/{Key}: the tenure ended under a command; the next owner classifies it.", tenure.Kind, tenure.Key);
        }
        catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.UniqueViolation && exception.ConstraintName == "command_one_taken")
        {
            logger.LogInformation("{Kind}/{Key}: a take lost the race for the key's one taken command (command_one_taken); the next poll reads again.", tenure.Kind, tenure.Key);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !token.IsCancellationRequested)
        {
            logger.LogWarning(exception, "{Kind}/{Key}: the command step failed; the next poll reads again.", tenure.Kind, tenure.Key);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "{Kind}/{Key}: the command step ended with the tenure or the host.", tenure.Kind, tenure.Key);
        }
    }

    private async Task<bool> TakeAndRunAsync(CommandTenure tenure, KeyState state, long id, CancellationToken host, CancellationToken cancellationToken)
    {
        var (taken, cancelled) = await StatementBound.RunAsync(
            bound => rows.TakeAsync(tenure.Owner, id, bound), optionsAccessor, time, cancellationToken).ConfigureAwait(false);
        var action = cancelled ? TakeAction.Unknown : taken!.Action;
        switch (action)
        {
            case TakeAction.Run:
                return await RunAsync(tenure, state, taken!.Row!, host, cancellationToken).ConfigureAwait(false);
            case TakeAction.Unknown:
                logger.LogWarning("{Kind}/{Key}: the take of command {Id} has an unknown outcome; the next poll decides.", tenure.Kind, tenure.Key, id);
                return false;
            default:
                return false;
        }
    }

    private async Task<bool> ClassifyAsync(CommandTenure tenure, KeyState state, ActiveCommandRow row, CancellationToken host, CancellationToken cancellationToken)
    {
        var policy = handlers.PolicyOf(tenure.Kind)!.Value;
        var oldToken = row.TakenToken!.Value;
        switch (ClassifyDecisions.Classify(policy, oldToken, tenure.Token))
        {
            case ClassifyAction.Retake:
            {
                var (retaken, cancelled) = await StatementBound.RunAsync(
                    bound => rows.RetakeAsync(tenure.Owner, row.Id, oldToken, bound), optionsAccessor, time, cancellationToken).ConfigureAwait(false);
                switch (cancelled ? TakeAction.Unknown : retaken!.Action)
                {
                    case TakeAction.Run:
                        return await RunAsync(tenure, state, retaken!.Row!, host, cancellationToken).ConfigureAwait(false);
                    case TakeAction.Unknown:
                        logger.LogWarning("{Kind}/{Key}: the re-take of command {Id} has an unknown outcome; the next poll decides.", tenure.Kind, tenure.Key, row.Id);
                        return false;
                    default:
                        return false;
                }
            }

            case ClassifyAction.MarkInDoubt:
            {
                var (marked, cancelled) = await StatementBound.RunAsync(
                    bound => rows.MarkInDoubtAsync(tenure.Owner, row.Id, oldToken, bound), optionsAccessor, time, cancellationToken).ConfigureAwait(false);
                switch (cancelled ? MarkAction.Unknown : marked)
                {
                    case MarkAction.Marked:
                        logger.LogWarning("{Kind}/{Key}: command {Id}, taken by an older tenure (token {Token}), is in doubt and is not run again.", tenure.Kind, tenure.Key, row.Id, oldToken);
                        await NudgeWaitersAsync(tenure.Kind, tenure.Key, cancellationToken).ConfigureAwait(false);
                        return true;
                    case MarkAction.Unknown:
                        logger.LogWarning("{Kind}/{Key}: the in-doubt verdict on command {Id} has an unknown outcome; the next poll decides.", tenure.Kind, tenure.Key, row.Id);
                        return false;
                    default:
                        return false;
                }
            }

            default:
                return false;
        }
    }

    private async Task<bool> RunAsync(CommandTenure tenure, KeyState state, ActiveCommandRow row, CancellationToken host, CancellationToken cancellationToken)
    {
        var handler = handlers.For(tenure.Kind)!;
        await points.ReachAsync(CommandsFaultPoints.AfterTake, cancellationToken).ConfigureAwait(false);

        tenure.Owner.Start();

        state.Flight = new KeyFlight(tenure.Token, row.Id, FlightPhase.Running);
        state.Outcome = null;
        var command = new DurableCommand(row.Id, tenure.Kind, tenure.Key, row.IdempotencyKey, row.Payload) { Owner = tenure.Context };

        var handling = Start(handler, command, scopes, host, tenure.Stopped);
        lock (state)
        {
            state.Handling = handling;
        }

        var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using (tenure.Stopped.Register(static s => ((TaskCompletionSource)s!).TrySetResult(), lost))
        {
            await Task.WhenAny(handling, lost.Task).ConfigureAwait(false);
        }

        var end = handling.IsCompletedSuccessfully ? HandlerEnd.Returned : handling.IsFaulted ? HandlerEnd.Threw : handling.IsCanceled ? HandlerEnd.Cancelled : HandlerEnd.Abandoned;
        var stopRequested = tenure.Stopped.IsCancellationRequested || host.IsCancellationRequested || applicationStoppingToken.IsCancellationRequested;
        switch (HandlerDecisions.After(end, stopRequested))
        {
            case HandlerAction.Complete:
                state.Outcome = CommandOutcome.Returned(handling.Result);
                state.Flight = new KeyFlight(tenure.Token, row.Id, FlightPhase.Returned);
                break;
            case HandlerAction.CompleteWithError:
                var error = handling.IsFaulted ? CommandText.ErrorOf(handling.Exception!) : CommandText.Cancelled;
                logger.LogWarning(handling.Exception, "{Kind}/{Key}: the handler of command {Id} threw; the command completes with its error.", tenure.Kind, tenure.Key, row.Id);
                state.Outcome = new CommandOutcome(default, error);
                state.Flight = new KeyFlight(tenure.Token, row.Id, FlightPhase.Faulted);
                break;
            default:
                state.Flight = new KeyFlight(tenure.Token, row.Id, FlightPhase.Failed);
                if (end != HandlerEnd.Abandoned)
                {
                    logger.LogInformation(handling.Exception, "{Kind}/{Key}: the handler of command {Id} ended after the tenure's or the host's stop was requested; the command stays taken.", tenure.Kind, tenure.Key, row.Id);
                }

                _ = handling.ContinueWith(
                    static (t, state) =>
                    {
                        var (pickup, kind, key, id) = ((CommandPickup, string, string, long))state!;
                        if (t.IsFaulted)
                        {
                            pickup.logger.LogWarning(t.Exception, "{Kind}/{Key}: the handler of command {Id}, abandoned by the stop, ended with an exception; the command stays taken.", kind, key, id);
                        }
                        else if (t.IsCanceled)
                        {
                            pickup.logger.LogDebug("{Kind}/{Key}: the handler of command {Id}, abandoned by the stop, ended cancelled.", kind, key, id);
                        }

                        pickup.Nudge();
                    },
                    (this, tenure.Kind, tenure.Key, row.Id),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                return false;
        }

        await points.ReachAsync(CommandsFaultPoints.BeforeComplete, cancellationToken).ConfigureAwait(false);
        return await CompleteAsync(tenure, state, row.Id, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> CompleteAsync(CommandTenure tenure, KeyState state, long id, CancellationToken cancellationToken)
    {
        CompleteAction action;
        try
        {
            var (completed, cancelled) = await StatementBound.RunAsync(
                bound => rows.CompleteAsync(tenure.Owner, id, state.Outcome ?? default, bound), optionsAccessor, time, cancellationToken).ConfigureAwait(false);
            action = cancelled ? CompleteAction.Unknown : completed;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not OwnershipLostException && !cancellationToken.IsCancellationRequested && CommandText.IsPermanent(exception))
        {
            logger.LogError(exception, "{Kind}/{Key}: the database refused the outcome of command {Id} for its content; the command completes with a fixed error at the next poll.", tenure.Kind, tenure.Key, id);
            state.Outcome = new CommandOutcome(default, CommandText.Unstorable);
            return false;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException and not OwnershipLostException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "{Kind}/{Key}: the completion of command {Id} got no answer; the next poll completes it again.", tenure.Kind, tenure.Key, id);
            return false;
        }

        switch (action)
        {
            case CompleteAction.Completed:
                state.Flight = null;
                state.Outcome = null;
                await NudgeWaitersAsync(tenure.Kind, tenure.Key, cancellationToken).ConfigureAwait(false);
                return true;
            case CompleteAction.Refused:
                logger.LogWarning("{Kind}/{Key}: the completion of command {Id} matched no row.", tenure.Kind, tenure.Key, id);
                state.Flight = null;
                state.Outcome = null;
                return false;
            default:
                logger.LogWarning("{Kind}/{Key}: the completion of command {Id} has an unknown outcome; the next poll decides.", tenure.Kind, tenure.Key, id);
                return false;
        }
    }

    private async ValueTask NudgeWaitersAsync(string kind, string key, CancellationToken cancellationToken)
    {
        try
        {
            await wakeups.PublishAsync(Wakeup.Wakeup.Nudge(WakeupKind.CommandNudge, CommandNudges.Completed(kind, key)), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(exception, "The completion nudge for {Kind}/{Key} was not published; the waiting callers poll.", kind, key);
        }
    }

    private static Task<ReadOnlyMemory<byte>> Start(
        IDurableCommandHandler handler, DurableCommand command, IClusterScopeSource scopes, CancellationToken cancellationToken, CancellationToken stopped)
    {
        if (ExecutionContext.IsFlowSuppressed())
        {
            return Task.Run(() => InScopeAsync(handler, command, scopes, cancellationToken, stopped), CancellationToken.None);
        }

        using (ExecutionContext.SuppressFlow())
        {
            return Task.Run(() => InScopeAsync(handler, command, scopes, cancellationToken, stopped), CancellationToken.None);
        }
    }

    private static async Task<ReadOnlyMemory<byte>> InScopeAsync(
        IDurableCommandHandler handler, DurableCommand command, IClusterScopeSource scopes, CancellationToken cancellationToken, CancellationToken stopped)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stopped);
        using var scope = scopes.Begin(ClusterScopeKind.Background);
        return await handler.HandleAsync(command, linked.Token).ConfigureAwait(false);
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var next = NewWake();
            Interlocked.Exchange(ref wake, next);
            try
            {
                await PassAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogError(exception, "The command poll failed; the next poll runs again.");
            }

            Interlocked.Increment(ref polls);

            try
            {
                await next.Task.WaitAsync(options.PlacementInterval, time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private CancellationTokenSource BoundSource() => new(options.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);

    private Bounded Bound(CancellationToken cancellationToken)
    {
        var timer = BoundSource();
        return new Bounded(timer, CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token));
    }

    private static TaskCompletionSource NewWake() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Bounded(CancellationTokenSource timer, CancellationTokenSource linked) : IDisposable
    {
        public CancellationToken Token => linked.Token;

        public void Dispose()
        {
            linked.Dispose();
            timer.Dispose();
        }
    }

    private sealed class KeyState
    {
        public Task? Step { get; set; }

        public Task? Handling { get; set; }

        public KeyFlight? Flight { get; set; }

        public CommandOutcome? Outcome { get; set; }
    }
}
