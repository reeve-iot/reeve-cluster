using Reeve.Cluster.Status;

namespace Reeve.Cluster.Log;

internal sealed class ApplierStatusSection(Applier applier) : IStatusSectionSource
{
    public const string Name = "applier";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var status = applier.Status;
        var values = new Dictionary<string, object?>
        {
            ["cursorSeq"] = status.Cursor.Seq,
            ["cursorToken"] = status.Cursor.Token,
            ["generation"] = status.Generation,
            ["observedHead"] = status.ObservedHead,
            ["stalled"] = status.StalledFor is not null,
            ["stalledForSeconds"] = status.StalledFor?.TotalSeconds,
            ["failingType"] = status.FailingType,
            ["firstUnappliedSeq"] = status.FirstUnappliedSeq,
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}
