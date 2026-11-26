using Microsoft.Extensions.DependencyInjection;

namespace Reeve.Cluster.Hosting;

internal sealed record ClusterAreaSelection(ClusterAreas Areas)
{
    public static ClusterAreaSelection Of(ClusterAreas requested) =>
        new((requested & ~ClusterAreas.OwnedWork) != ClusterAreas.None ? requested | ClusterAreas.OwnedWork : requested);

    public bool Has(ClusterAreas area) => (Areas & area) == area;
}

internal sealed record PrimitivesRequested(string Primitive);

internal sealed class AskAreaDescriptors(IReadOnlyList<ServiceDescriptor> descriptors)
{
    public IReadOnlyList<ServiceDescriptor> Descriptors { get; } = descriptors;
}
