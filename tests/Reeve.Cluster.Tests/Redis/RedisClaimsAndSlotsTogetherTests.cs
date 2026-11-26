using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;
using Unit.Diagnostics;

namespace Unit.Redis;

[Collection(MetricsCollection.Name)]
public sealed class RedisClaimsAndSlotsTogetherTests
{
    private static void Required(ClusterOptions o)
    {
        o.ReplicaId = "replica-1";
        o.ConnectionString = "Host=db;Database=app";
        o.ReconcileTimeout = TimeSpan.FromSeconds(10);
    }

    [Fact]
    public async Task A_claim_and_a_slot_registered_together_with_different_policies_both_resolve_with_both_floors_and_one_startup_check()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddSingleton(DispatchProxy.Create<IConnectionMultiplexer, SlotMetricsTests.NullMultiplexer>());
        services.AddReeveClusterRedisClaims(OnRedisUnavailable.Grant);
        services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);

        Assert.Equal(["claim", "slot"], services.Select(d => d.ImplementationInstance).OfType<RedisRequirement>().Select(r => r.Primitive).Order());
        Assert.Single(services, d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(RedisStartupCheck));
        await using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IRedisClaims>());
        Assert.NotNull(provider.GetRequiredService<IRedisSlots>());
        Assert.NotNull(provider.GetRequiredService<IClusterClaims>());
        Assert.NotNull(provider.GetRequiredService<IClusterSlots>());
    }
}
