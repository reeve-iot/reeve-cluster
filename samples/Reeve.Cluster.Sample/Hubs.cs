using Microsoft.AspNetCore.SignalR;

namespace Reeve.Cluster.Sample;

public sealed class FlagHub(IClusterScopeAccessor scopes) : Hub
{
    public bool? GetFlag(string name) =>
        scopes.Current?.PinnedGeneration?.Get<FlagState>("flag").Flags.TryGetValue(name, out var enabled) == true ? enabled : null;

    public ConfigView GetConfig() => ConfigView.Of(scopes);
}
