using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Status;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Log;

internal static class LogServiceCollectionExtensions
{
    public static IServiceCollection AddClusterLog(this IServiceCollection services)
    {
        services.TryAddSingleton<IFaultPoints>(NoFaultPoints.Instance);
        services.TryAddSingleton<IModelEventSink>(NoModelEvents.Instance);

        services.TryAddSingleton<IWakeupChannel, InProcessWakeupChannel>();

        services.TryAddSingleton<FenceSteps>();
        services.TryAddSingleton<ClusterScopeSource>();
        services.TryAddSingleton<IClusterScopeSource>(sp => sp.GetRequiredService<ClusterScopeSource>());
        services.TryAddSingleton<IClusterScopeAccessor>(sp => sp.GetRequiredService<ClusterScopeSource>());

        services.TryAddSingleton<CommitRefusal>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IFenceParticipant, AppendRefusalParticipant>());
        services.TryAddSingleton<EfTransactions>();
        services.TryAddSingleton<ChangeLog>();
        services.TryAddSingleton<IChangeLog>(sp => sp.GetRequiredService<ChangeLog>());
        services.TryAddSingleton<PostCommitWakeup>();

        services.TryAddSingleton(sp => new GenerationStore(
            ReconcilerGraph.Build([.. sp.GetServices<IReconciler>()]),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<IModelEventSink>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<GenerationStore>>()));
        services.TryAddSingleton<IGenerationSource>(sp => sp.GetRequiredService<GenerationStore>());

        services.TryAddSingleton<IClusterGenerations, ClusterGenerations>();
        services.TryAddSingleton<ApplierDataSource>();
        services.TryAddSingleton<Applier>();
        services.TryAddSingleton<IMemberLoad>(sp => sp.GetRequiredService<Applier>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IFenceParticipant, ApplierStopParticipant>());

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, ApplierStatusSection>());

        services.TryAddSingleton<ReadIndexDataSource>();
        services.TryAddSingleton<IReadIndexStore>(sp => new ReadIndexStore(sp.GetRequiredService<ReadIndexDataSource>().Source));
        services.TryAddSingleton<ReadIndex>();

        services.TryAddSingleton<AckDataSource>();
        services.TryAddSingleton<AckRecorder>();
        services.TryAddSingleton<ChangeSkip>();
        services.TryAddSingleton<IChangeSkip>(sp => sp.GetRequiredService<ChangeSkip>());

        services.TryAddSingleton(sp => ActivatorUtilities.CreateInstance<ChangeAck>(sp, sp.GetRequiredService<ReadIndexDataSource>().Source));
        services.TryAddSingleton<IChangeAck>(sp => sp.GetRequiredService<ChangeAck>());

        services.TryAddSingleton<SkipMonitor>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SkipMonitor>(sp => sp.GetRequiredService<SkipMonitor>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, SkipMonitor>(sp => sp.GetRequiredService<SkipMonitor>()));

        services.TryAddSingleton<Retention>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, Retention>(sp => sp.GetRequiredService<Retention>()));
        return services;
    }
}
