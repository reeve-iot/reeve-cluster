using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed class ClusterCaptureOptionsExtension : IDbContextOptionsExtension
{
    private readonly ClusterCaptureOptionsExtensionInfo info;

    public ClusterCaptureOptionsExtension(IClusterMapping mapping)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        Mapping = mapping;
        info = new ClusterCaptureOptionsExtensionInfo(this);
    }

    public IClusterMapping Mapping { get; }

    public DbContextOptionsExtensionInfo Info => info;

    public void ApplyServices(IServiceCollection services)
    {
    }

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ClusterCaptureOptionsExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => false;

        public override string LogFragment => string.Empty;

        public override int GetServiceProviderHashCode() => 0;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ClusterCaptureOptionsExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
        }
    }
}
