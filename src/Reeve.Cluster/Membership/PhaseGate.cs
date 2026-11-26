using Reeve.Cluster.Core;

namespace Reeve.Cluster.Membership;

internal enum FenceTrigger
{
    FenceStart,

    ApplierStall,

    StuckAttempt,

    FatalFault,

    ZeroRowRenewal,

    Timeline,

    UnansweredActivation,
}

internal readonly record struct FenceRequest(FenceTrigger Trigger, string? Detail)
{
    public FenceRequest(FenceTrigger trigger)
        : this(trigger, null)
    {
    }
}

internal enum PhaseEventKind
{
    Transition,

    DrainStep4,
}

internal readonly record struct PhaseEvent(PhaseEventKind Kind, ReplicaPhase From, ReplicaPhase To);

internal enum ServingCheckOutcome
{
    Serving,

    NotServing,

    FenceStartPassed,
}

internal readonly record struct ServingCheckResult(ServingCheckOutcome Outcome, ReplicaPhase Phase)
{
    public bool IsServing => Outcome == ServingCheckOutcome.Serving;

    public long? Epoch { get; init; }
}

internal sealed class NotServingException(ServingCheckResult result)
    : InvalidOperationException($"The replica is not serving ({result.Outcome}, phase {result.Phase}).")
{
    public ServingCheckResult Result { get; } = result;
}

internal sealed record ReadinessReason(string Code, string Description);

internal interface IReadinessReasons
{
    IReadOnlyList<ReadinessReason> Current { get; }

    void Raise(ReadinessReason reason);

    void Clear(string code);
}

internal interface IPhaseGate
{
    ReplicaPhase Phase { get; }

    long? FenceStartTimestamp { get; }

    ServingCheckResult CheckServing();

    FenceDecision RequestFence(FenceRequest request);

    FenceDecision ReportFatalFault(string reason);

    bool ExitAfterFence { get; }

    IDisposable OnStepZero(Action<FenceRequest> hook);

    IDisposable OnFenceRequested(Action<FenceRequest> handler);

    IDisposable OnExitRequested(Action<FenceRequest> handler);

    IDisposable Subscribe(Action<PhaseEvent> handler);

    IReadinessReasons Readiness { get; }

    bool DrainStep4Passed => false;
}

internal static class PhaseGateExtensions
{
    public static void EnsureServing(this IPhaseGate gate)
    {
        ArgumentNullException.ThrowIfNull(gate);
        var result = gate.CheckServing();
        if (!result.IsServing)
        {
            throw new NotServingException(result);
        }
    }
}
