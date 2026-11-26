using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Commands;

internal sealed record CommandTenure(string Kind, string Key, long Token, IFencedOwner Owner, OwnedContext Context, CancellationToken Stopped);

internal interface ICommandTenures
{
    IReadOnlyList<CommandTenure> RunningOf(IReadOnlyCollection<string> kinds);

    event Action? TenureStarted;
}
