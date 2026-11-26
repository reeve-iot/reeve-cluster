namespace Reeve.Cluster.Wakeup;

internal enum WakeupKind
{
    Appended,

    Applied,

    OwnerNudge,

    CommandNudge,
}

internal readonly record struct Wakeup(WakeupKind Kind, long Seq, string? Target)
{
    public Wakeup(WakeupKind kind, long seq)
        : this(kind, seq, null)
    {
    }

    public static Wakeup Nudge(WakeupKind kind, string target) => new(kind, 0, target);
}

internal interface IWakeupChannel
{
    ValueTask PublishAsync(Wakeup wakeup, CancellationToken cancellationToken = default);

    IDisposable Subscribe(WakeupKind kind, Action<Wakeup> handler);
}
