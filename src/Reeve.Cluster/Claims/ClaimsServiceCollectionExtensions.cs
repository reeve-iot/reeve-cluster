using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Claims;

internal static class ClaimsServiceCollectionExtensions
{
    public static IServiceCollection AddClusterClaims(this IServiceCollection services)
    {
        services.TryAddSingleton<ClaimStore>(sp => new ClaimStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IClaimRows>(sp => sp.GetRequiredService<ClaimStore>());
        services.TryAddSingleton<IClaimGate>(sp => new LocalClaimGate(sp.GetRequiredService<LocalPhaseGate>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<ClaimScopes>();
        services.TryAddSingleton<ClusterClaims>(sp => new ClusterClaims(
            sp.GetRequiredService<IClaimRows>(),
            sp.GetRequiredService<IClaimGate>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<ClusterClaims>>(),
            sp.GetRequiredService<ClaimScopes>()));
        services.TryAddSingleton<IClusterClaims>(sp => sp.GetRequiredService<ClusterClaims>());

        services.TryAddSingleton<OnceStore>(sp => new OnceStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IOnceRows>(sp => sp.GetRequiredService<OnceStore>());
        services.TryAddSingleton<OnceEffects>(sp => new OnceEffects(sp.GetServices<OnceEffectRegistration>()));
        services.TryAddSingleton<ClusterOnce>(sp => new ClusterOnce(
            sp.GetRequiredService<IOnceRows>(),
            sp.GetRequiredService<IClaimGate>(),
            sp.GetRequiredService<OnceEffects>(),
            sp.GetRequiredService<IServiceScopeFactory>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<ClusterOnce>>()));
        services.TryAddSingleton<IClusterOnce>(sp => sp.GetRequiredService<ClusterOnce>());
        services.TryAddSingleton<OnceSweeper>(sp => new OnceSweeper(
            sp.GetRequiredService<IOnceRows>(),
            sp.GetRequiredService<IClaimGate>(),
            sp.GetRequiredService<OnceEffects>(),
            sp.GetRequiredService<IServiceScopeFactory>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<OnceSweeper>>()));
        if (!services.Any(d => d.ImplementationInstance is UnitKind { Kind: OnceSweeper.Kind }))
        {
            services.AddClusterUnit<OnceSweeper>(OnceSweeper.Kind, o => o.Convergent = true);
        }

        services.TryAddSingleton<RetentionStore>(sp => new RetentionStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IRetentionRows>(sp => sp.GetRequiredService<RetentionStore>());
        services.TryAddSingleton<PrimitiveRetention>(sp => new PrimitiveRetention(
            sp.GetRequiredService<IRetentionRows>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<PrimitiveRetention>>()));
        if (!services.Any(d => d.ImplementationInstance is UnitKind { Kind: PrimitiveRetention.Kind }))
        {
            services.AddClusterUnit<PrimitiveRetention>(PrimitiveRetention.Kind, o => o.Convergent = true);
        }

        services.TryAddSingleton<FlightStore>(sp => new FlightStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IFlightRows>(sp => sp.GetRequiredService<FlightStore>());
        services.TryAddSingleton<ClusterSingleFlight>(sp => new ClusterSingleFlight(
            sp.GetRequiredService<IFlightRows>(),
            sp.GetRequiredService<IClaimGate>(),
            sp.GetRequiredService<ClaimScopes>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<ClusterSingleFlight>>()));
        services.TryAddSingleton<IClusterSingleFlight>(sp => sp.GetRequiredService<ClusterSingleFlight>());
        services.TryAddSingleton<BudgetStore>(sp => new BudgetStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<IBudgetRows>(sp => sp.GetRequiredService<BudgetStore>());
        services.TryAddSingleton<ClusterBudget>(sp => new ClusterBudget(
            sp.GetRequiredService<IBudgetRows>(),
            sp.GetRequiredService<IClaimGate>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<ILogger<ClusterBudget>>()));
        services.TryAddSingleton<IClusterBudget>(sp => sp.GetRequiredService<ClusterBudget>());
        services.TryAddSingleton<CounterStore>(sp => new CounterStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<ICounterRows>(sp => sp.GetRequiredService<CounterStore>());
        services.TryAddSingleton<ClusterCounter>(sp => new ClusterCounter(
            sp.GetRequiredService<ICounterRows>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<ClusterCounter>>()));
        services.TryAddSingleton<IClusterCounter>(sp => sp.GetRequiredService<ClusterCounter>());

        return services;
    }
}

internal sealed class LocalClaimGate(LocalPhaseGate gate, IOptions<ClusterOptions> options) : IClaimGate
{
    public ClaimSeat? CheckServing()
    {
        var check = gate.CheckServing();
        if (!check.IsServing || check.Epoch is not { } epoch || !gate.TryGetTimeline(epoch, out var timeline))
        {
            return null;
        }

        return new ClaimSeat(options.Value.ReplicaId ?? throw new InvalidOperationException("ReplicaId is required."), epoch, timeline);
    }

    public void OnOtherTimeline(long epoch) => gate.OnOtherTimeline(epoch);
}
