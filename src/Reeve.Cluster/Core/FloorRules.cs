namespace Reeve.Cluster.Core;

internal enum VersionRaiseDecision
{
    AlreadyAtOrAbove,

    Raise,

    RefusedByMember,

    FloorNotSemVer,
}

internal enum VersionLowerDecision
{
    Lower,

    NotAbove,
}

internal static class FloorRules
{
    public static VersionRaiseDecision DecideVersionRaise(string version, string? floorRead, IEnumerable<string> leasedMemberVersions)
    {
        ArgumentNullException.ThrowIfNull(leasedMemberVersions);
        var required = SemanticVersion.Parse(version);
        if (floorRead is not null)
        {
            if (!SemanticVersion.TryParse(floorRead, out var floor))
            {
                return VersionRaiseDecision.FloorNotSemVer;
            }

            if (floor!.CompareTo(required) >= 0)
            {
                return VersionRaiseDecision.AlreadyAtOrAbove;
            }
        }

        return AllAtLeast(required, leasedMemberVersions) ? VersionRaiseDecision.Raise : VersionRaiseDecision.RefusedByMember;
    }

    public static bool AllAtLeast(string version, IEnumerable<string> memberVersions) =>
        AllAtLeast(SemanticVersion.Parse(version), memberVersions);

    public static VersionLowerDecision DecideVersionLower(string? newVersion, string? floorRead)
    {
        if (floorRead is null)
        {
            return VersionLowerDecision.NotAbove;
        }

        if (newVersion is null || !SemanticVersion.TryParse(floorRead, out var floor))
        {
            return VersionLowerDecision.Lower;
        }

        return SemanticVersion.Parse(newVersion).CompareTo(floor) < 0 ? VersionLowerDecision.Lower : VersionLowerDecision.NotAbove;
    }

    private static bool AllAtLeast(SemanticVersion required, IEnumerable<string> memberVersions)
    {
        ArgumentNullException.ThrowIfNull(memberVersions);
        foreach (var member in memberVersions)
        {
            if (!SemanticVersion.TryParse(member, out var own) || own!.CompareTo(required) < 0)
            {
                return false;
            }
        }

        return true;
    }
}
