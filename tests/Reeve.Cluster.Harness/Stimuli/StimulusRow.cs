namespace Reeve.Cluster.Harness.Stimuli;

internal enum StimulusKind
{
    Point,

    Operation,

    Elapse,

    Follows,
}

internal enum StimulusSubject
{
    Replica,

    Write,

    Read,

    Fleet,
}

internal enum TimeAdvance
{
    None,

    LetTimePass,

    ReplicaClock,
}

internal enum HarnessOperation
{
    PauseReplica,
    ResumeReplica,
    CrashReplica,
    RestartReplica,
    StopReplica,

    StopReplicaEarly,
    OperatorDelete,
    RaiseProtocolFloor,
    ClockShift,
    ReconcilerThrow,
    ReconcilerHang,
    ReconcilerRelease,

    FixReconcilerFault,
    ReportFatalFault,

    ReportFatalFaultWhileFenced,

    StartWrite,

    SaveInForeignTransaction,

    RollbackWrite,

    AcknowledgeAtCommit,

    StartRead,

    RestoreDatabase,
}

internal sealed record StimulusPoint(FaultPoint Point, string? PlacedBy = null);

internal sealed record StimulusRow(
    string Action,
    StimulusSubject Subject,
    StimulusKind Kind,
    IReadOnlyList<StimulusPoint> Points,
    HarnessOperation? Operation,
    TimeAdvance Advance,
    string Stimulus,
    string? Enabling,
    string? PlacedBy,
    string? ProvenBy = null)
{
    public StimulusRow Proven(string testName) => this with { PlacedBy = null, ProvenBy = testName };

    public bool IsReady => PlacedBy is null && Points.All(p => p.PlacedBy is null);

    public IReadOnlyList<string> Waiting => [.. Points.Select(p => p.PlacedBy).Append(PlacedBy).Where(t => t is not null).Select(t => t!).Distinct()];

    public static StimulusPoint[] Of(string? placedBy, params FaultPoint[] points) => [.. points.Select(p => new StimulusPoint(p, placedBy))];

    public static StimulusRow At(string action, StimulusSubject subject, string stimulus, StimulusPoint[] points, HarnessOperation? operation = null, string? enabling = null, string? placedBy = null, TimeAdvance advance = TimeAdvance.None) =>
        new(action, subject, StimulusKind.Point, points, operation, advance, stimulus, enabling, placedBy);

    public static StimulusRow Do(string action, StimulusSubject subject, string stimulus, HarnessOperation operation, string? enabling = null, string? placedBy = null) =>
        new(action, subject, StimulusKind.Operation, [], operation, TimeAdvance.None, stimulus, enabling, placedBy);

    public static StimulusRow Next(string action, StimulusSubject subject, string stimulus, string? enabling = null, string? placedBy = null) =>
        new(action, subject, StimulusKind.Follows, [], null, TimeAdvance.None, stimulus, enabling, placedBy);

    public static StimulusRow Over(string action, StimulusSubject subject, TimeAdvance advance, string stimulus, string? enabling = null, string? placedBy = null, StimulusPoint[]? points = null) =>
        new(action, subject, StimulusKind.Elapse, points ?? [], null, advance, stimulus, enabling, placedBy);
}
