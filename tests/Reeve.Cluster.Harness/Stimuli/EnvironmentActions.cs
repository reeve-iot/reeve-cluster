namespace Reeve.Cluster.Harness.Stimuli;

internal static class EnvironmentActions
{
    public const string Pause = nameof(Pause);
    public const string Resume = nameof(Resume);
    public const string Crash = nameof(Crash);
    public const string Restart = nameof(Restart);
    public const string StopEarly = nameof(StopEarly);
    public const string OperatorDelete = nameof(OperatorDelete);
    public const string FloorRaise = nameof(FloorRaise);
    public const string FixFault = nameof(FixFault);
    public const string Restore = nameof(Restore);
    public const string Retention = nameof(Retention);
    public const string Tick = nameof(Tick);
    public const string Expire = nameof(Expire);

    public const string ReplicaKey = "replica";
}
