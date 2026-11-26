using Microsoft.Extensions.Options;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Owned;

internal sealed record HeldTenure(string Kind, string Key, long Token, long Epoch, bool Running, bool Ready);

internal sealed class OwnedStatusSection(UnitRuntime runtime, IOptions<ClusterOptions> options) : IStatusSectionSource
{
    public const string Name = "owned";

    string IStatusSectionSource.Name => Name;

    public ValueTask<StatusSection> CollectAsync(CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, object?>
        {
            ["units"] = runtime.HeldTenures().Select(t => new Dictionary<string, object?>
            {
                ["kind"] = t.Kind,
                ["key"] = t.Key,
                ["token"] = t.Token,
                ["epoch"] = t.Epoch,
                ["running"] = t.Running,
                ["ready"] = t.Ready,
            }).ToList(),
            ["releaseGraceSeconds"] = options.Value.ReleaseGrace.TotalSeconds,
        };
        return ValueTask.FromResult(new StatusSection { Name = Name, Values = values });
    }
}
