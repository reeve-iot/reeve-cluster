using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Core;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Log;
using Reeve.Cluster.Status;

namespace Reeve.Cluster.Membership;

internal static class MembershipServiceCollectionExtensions
{
    public static IServiceCollection AddClusterMembership(this IServiceCollection services)
    {
        services.TryAddSingleton(sp => new ClusterDataSource(sp.GetRequiredService<IOptions<ClusterOptions>>().Value));

        services.TryAddSingleton<IFaultPoints>(NoFaultPoints.Instance);
        services.TryAddSingleton<IModelEventSink>(NoModelEvents.Instance);

        services.TryAddSingleton<LocalPhaseGate>();
        services.TryAddSingleton<IPhaseGate>(sp => sp.GetRequiredService<LocalPhaseGate>());

        services.TryAddSingleton<IReplicaPhase, ReplicaPhaseReader>();
        services.TryAddSingleton(_ => MemberIdentity.ForThisProcess());
        services.TryAddSingleton<RenewalDataSource>();
        services.TryAddSingleton<MemberWriteDataSource>();
        services.TryAddSingleton<IMemberStore, MemberStore>();
        services.TryAddSingleton<RenewalLoop>();

        services.TryAddSingleton<ICapabilityStore, CapabilityStore>();
        services.TryAddSingleton<ClusterCapabilitiesRegistry>();
        services.TryAddSingleton<IClusterCapabilities>(sp => sp.GetRequiredService<ClusterCapabilitiesRegistry>());

        services.TryAddSingleton<IFenceStore, FenceStore>();
        services.TryAddSingleton<FencePassConnection>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IFenceParticipant, FenceSinkParticipant>());
        services.TryAddSingleton<FenceCoordinator>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, FenceCoordinator>(sp => sp.GetRequiredService<FenceCoordinator>()));
        services.TryAddSingleton<IActivationStore, ActivationStore>();
        services.TryAddSingleton<MemberActivation>();
        services.TryAddSingleton<IMemberActivation>(sp => sp.GetRequiredService<MemberActivation>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, PhaseStatusSection>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, FloorStatusSection>());

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, LeaseStatusSection>());
        services.TryAddSingleton<IStatusStore>(sp => new StatusStore(
            sp.GetRequiredService<ClusterDataSource>(), sp.GetRequiredService<ReadIndexDataSource>().Source, sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IClusterStatus, ClusterStatus>();
        services.TryAddSingleton<StatusReporter>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, StatusReporter>(sp => sp.GetRequiredService<StatusReporter>()));

        services.TryAddSingleton<IFloorStore, FloorStore>();
        services.TryAddSingleton<IClusterVersionGate, ClusterVersionGate>();
        services.TryAddSingleton<IClusterFloorCommand, ClusterFloorCommand>();
        services.TryAddSingleton(UpgradePathDeclaration.ThisRelease);
        services.TryAddSingleton<IUpgradePathGuard, UpgradePathGuard>();

        services.TryAddSingleton<ReplicationMonitor>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ReplicationMonitor>(sp => sp.GetRequiredService<ReplicationMonitor>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStatusSectionSource, ReplicationMonitor>(sp => sp.GetRequiredService<ReplicationMonitor>()));

        services.TryAddSingleton<DrainCoordinator>();

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, MembershipDriver>());
        return services;
    }
}
