using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Core;

internal enum Admission
{
    Admitted,

    BelowVersionFloor,

    VersionNotSemVer,

    FloorNotSemVer,
}

internal enum ActivationOutcome
{
    NotCaughtUp,

    Committed,

    RefusedByVersionFloor,

    RefusedByProtocolFloor,

    Abandoned,

    LeftCatchingUp,

    Unanswered,
}

internal static class ActivationRules
{
    public const string ProtocolFloorReason = "activation.protocol-floor";

    public const string VersionFloorReason = "activation.version-floor";

    public const string FailingReason = "activation.failing";

    public static bool IsCaughtUp(bool cursorValid, long entriesAfterCursor, long threshold) =>
        cursorValid && entriesAfterCursor <= threshold;

    public static Admission Admit(string version, string? versionFloorRead)
    {
        if (versionFloorRead is null)
        {
            return Admission.Admitted;
        }

        if (!SemanticVersion.TryParse(versionFloorRead, out var floor))
        {
            return Admission.FloorNotSemVer;
        }

        if (!SemanticVersion.TryParse(version, out var own))
        {
            return Admission.VersionNotSemVer;
        }

        return own!.CompareTo(floor) < 0 ? Admission.BelowVersionFloor : Admission.Admitted;
    }

    public static ActivationOutcome OnZeroRows(int protocol, int protocolFloorRead) =>
        protocol < protocolFloorRead ? ActivationOutcome.RefusedByProtocolFloor : ActivationOutcome.Abandoned;

    public static MemberTrigger? TriggerFor(ActivationOutcome outcome) => outcome switch
    {
        ActivationOutcome.Committed => MemberTrigger.ActivationCommitted,
        ActivationOutcome.Abandoned => MemberTrigger.ActivationAbandoned,
        ActivationOutcome.RefusedByProtocolFloor or ActivationOutcome.RefusedByVersionFloor => MemberTrigger.ActivationRefusedByFloor,
        _ => null,
    };

    public static string? ReasonCode(ActivationOutcome outcome) => outcome switch
    {
        ActivationOutcome.RefusedByProtocolFloor => ProtocolFloorReason,
        ActivationOutcome.RefusedByVersionFloor => VersionFloorReason,
        _ => null,
    };

    public static bool HasReachedHead(Cursor applied, long head) => applied.Seq >= head;
}
