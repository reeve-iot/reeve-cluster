using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Ask;

internal sealed class AskHandlers
{
    private readonly Lazy<Resolved> resolved;

    public AskHandlers(IReadOnlyDictionary<string, IAskHandler> keyed, IAskHandler? member)
    {
        ArgumentNullException.ThrowIfNull(keyed);
        resolved = new(new Resolved(keyed, member));
    }

    public AskHandlers(Func<(IReadOnlyDictionary<string, IAskHandler> Keyed, IAskHandler? Member)> resolve)
    {
        ArgumentNullException.ThrowIfNull(resolve);
        resolved = new(() =>
        {
            var (keyed, member) = resolve();
            return new Resolved(keyed, member);
        });
    }

    public IAskHandler? Member => resolved.Value.Member;

    public void Resolve() => _ = resolved.Value;

    public IAskHandler? For(string kind) => resolved.Value.Keyed.GetValueOrDefault(kind);

    private sealed record Resolved(IReadOnlyDictionary<string, IAskHandler> Keyed, IAskHandler? Member);
}

internal sealed class AskInbound(
    IPhaseGate gate,
    IClusterScopeSource scopes,
    IRunningTenures tenures,
    AskHandlers handlers,
    ILogger<AskInbound> logger) : IAskInbound
{
    private readonly ConcurrentDictionary<string, bool> warned = new(StringComparer.Ordinal);

    public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy)
    {
        var reservation = Reserve(delivery, answerBy);
        return ValueTask.FromResult(reservation.IsReserved ? AskTaking.Taken(reservation.Start()) : AskTaking.Decline(reservation.Reason!.Value));
    }

    public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy) => ValueTask.FromResult(Reserve(delivery, answerBy));

    private AskReservation Reserve(AskDelivery delivery, CancellationToken answerBy)
    {
        ArgumentNullException.ThrowIfNull(delivery);

        if (answerBy.IsCancellationRequested)
        {
            return AskReservation.Decline(DeclineReason.WaitEnded);
        }

        if (delivery.IsMember)
        {
            return handlers.Member is { } member
                ? AskReservation.Reserve(() => Start(member, new AskRequest(null, null, delivery.Payload), answerBy, default, keyed: false))
                : NoHandler("<member>", "a request to this replica");
        }

        if (delivery.Key is not { } key || delivery.Token is not { } token)
        {
            return AskReservation.Decline(DeclineReason.Absent);
        }

        if (handlers.For(delivery.Kind!) is not { } handler)
        {
            return NoHandler(delivery.Kind!, $"the requests for kind '{delivery.Kind}'");
        }

        if (!tenures.TryGetRunning(delivery.Kind!, key, token, out var stopped, out var owner))
        {
            return AskReservation.Decline(tenures.IsStopping(delivery.Kind!, key, token) ? DeclineReason.Stopping : DeclineReason.Absent);
        }

        if (!gate.CheckServing().IsServing)
        {
            return AskReservation.Decline(DeclineReason.NotServing);
        }

        return AskReservation.Reserve(() => Start(handler, new AskRequest(delivery.Kind, key, delivery.Payload) { Owner = owner }, answerBy, stopped, keyed: true));
    }

    private AskReservation NoHandler(string what, string description)
    {
        if (warned.TryAdd(what, true))
        {
            logger.LogWarning("No Ask handler is registered for {Requests}; they are declined here (register one with AddClusterAskHandler or AddClusterMemberAskHandler).", description);
        }

        return AskReservation.Decline(DeclineReason.NoHandler);
    }

    private Task<AskAnswer> Start(IAskHandler handler, AskRequest request, CancellationToken answerBy, CancellationToken stopped, bool keyed)
    {
        Task<ReadOnlyMemory<byte>> handling;
        if (ExecutionContext.IsFlowSuppressed())
        {
            handling = Task.Run(() => HandleInScopeAsync(handler, request, answerBy, stopped), CancellationToken.None);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
            {
                handling = Task.Run(() => HandleInScopeAsync(handler, request, answerBy, stopped), CancellationToken.None);
            }
        }

        return AnswerAsync(handling, answerBy, stopped, keyed);
    }

    private async Task<ReadOnlyMemory<byte>> HandleInScopeAsync(IAskHandler handler, AskRequest request, CancellationToken answerBy, CancellationToken stopped)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(answerBy, stopped);
        using var scope = scopes.Begin(ClusterScopeKind.Background);
        return await handler.HandleAsync(request, linked.Token).ConfigureAwait(false);
    }

    private async Task<AskAnswer> AnswerAsync(Task<ReadOnlyMemory<byte>> handling, CancellationToken answerBy, CancellationToken stopped, bool keyed)
    {
        _ = handling.ContinueWith(
            static (task, state) => ((ILogger<AskInbound>)state!).LogWarning(task.Exception, "An Ask handler threw."),
            logger,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        if (keyed)
        {
            var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = stopped.Register(static s => ((TaskCompletionSource)s!).TrySetResult(), lost);
            await Task.WhenAny(handling, lost.Task).ConfigureAwait(false);
        }
        else
        {
            await Task.WhenAny(handling).ConfigureAwait(false);
        }

        return AskOutcomes.AnswerOf(handling.IsCompletedSuccessfully, keyed && stopped.IsCancellationRequested, answerBy.IsCancellationRequested) switch
        {
            AskAnswerKind.Unanswered => new AskAnswer(AskAnswerKind.Unanswered, default, null),
            AskAnswerKind.Response => new AskAnswer(AskAnswerKind.Response, handling.Result, null),
            AskAnswerKind.OwnerLost => new AskAnswer(AskAnswerKind.OwnerLost, default, null),
            _ => new AskAnswer(AskAnswerKind.HandlerError, default, handling.Exception?.GetBaseException().Message ?? "The handler was cancelled."),
        };
    }
}
