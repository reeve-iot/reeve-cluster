namespace Reeve.Cluster.Harness.Stimuli;

internal static class StimulusTable
{
    public static IReadOnlyList<(string Area, IReadOnlyList<StimulusRow> Rows)> Areas { get; } =
    [
        ("Membership", MembershipStimuli.All),
        ("Log", LogStimuli.All),
        ("Environment", EnvironmentStimuli.All),
        ("Owned", OwnedStimuli.All),
    ];

    public static IReadOnlyList<StimulusRow> All { get; } = [.. Areas.SelectMany(a => a.Rows)];

    public static StimulusRow? Find(string action) => All.FirstOrDefault(r => r.Action == action);

    public static StimulusRow For(string action) =>
        Find(action) ?? throw new KeyNotFoundException($"The stimulus table has no row for the action '{action}'.");
}
