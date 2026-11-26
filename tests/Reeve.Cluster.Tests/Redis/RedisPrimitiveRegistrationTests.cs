using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;

namespace Unit.Redis;

public sealed class RedisPrimitiveRegistrationTests
{
    private static readonly OnRedisUnavailable[] Policies = [OnRedisUnavailable.Grant, OnRedisUnavailable.Refuse];

    public static IEnumerable<object[]> Registrations() =>
    [
        ["claims", typeof(IRedisClaims), typeof(IClusterClaims), true],
        ["single-flight", typeof(IRedisSingleFlight), typeof(IClusterSingleFlight), true],
        ["slots", typeof(IRedisSlots), typeof(IClusterSlots), true],
        ["budget", typeof(IRedisBudget), typeof(IClusterBudget), false],
        ["counter", typeof(IRedisCounter), typeof(IClusterCounter), false],
    ];

    private static IServiceCollection Register(string primitive, IServiceCollection services, OnRedisUnavailable policy) => primitive switch
    {
        "claims" => services.AddReeveClusterRedisClaims(policy),
        "single-flight" => services.AddReeveClusterRedisSingleFlight(policy),
        "slots" => services.AddReeveClusterRedisSlots(policy),
        "budget" => services.AddReeveClusterRedisBudget(policy),
        _ => services.AddReeveClusterRedisCounter(),
    };

    [Theory]
    [MemberData(nameof(Registrations))]
    public void A_Redis_primitive_is_registered_under_its_own_interface_and_leaves_the_PostgreSQL_one_alone(string primitive, Type redis, Type postgres, bool floor)
    {
        var services = new ServiceCollection();

        Register(primitive, services, OnRedisUnavailable.Refuse);

        Assert.Contains(services, d => d.ServiceType == redis);
        Assert.DoesNotContain(services, d => d.ServiceType == postgres);
        var requirement = Assert.Single(services.Select(d => d.ImplementationInstance).OfType<RedisRequirement>());
        Assert.Equal(floor, requirement.NeedsVersionFloor);
        Assert.Equal(primitive == "slots", requirement.RefusesUnreachable);
        Assert.Single(services, IsRedisStartupCheck);
    }

    [Theory]
    [InlineData("claims")]
    [InlineData("single-flight")]
    [InlineData("slots")]
    [InlineData("budget")]
    public void A_value_that_is_not_a_policy_is_a_named_configuration_error_at_registration_and_registers_nothing(string primitive)
    {
        var services = new ServiceCollection();

        var refusal = Assert.Throws<ClusterConfigurationException>(() => Register(primitive, services, default));

        Assert.Equal(ClusterConfigurationReason.RedisPolicyRequired, refusal.Reason);
        Assert.Empty(services);
    }

    [Theory]
    [InlineData("claims")]
    [InlineData("single-flight")]
    [InlineData("slots")]
    [InlineData("budget")]
    public void A_policy_that_is_not_one_is_refused_after_a_valid_registration_too(string primitive)
    {
        var services = new ServiceCollection();
        Register(primitive, services, OnRedisUnavailable.Refuse);
        var registered = services.Count;

        Assert.Throws<ClusterConfigurationException>(() => Register(primitive, services, (OnRedisUnavailable)7));
        Assert.Equal(registered, services.Count);
    }

    [Theory]
    [InlineData("claims")]
    [InlineData("single-flight")]
    [InlineData("slots")]
    [InlineData("budget")]
    public void A_second_registration_is_refused_and_not_ignored(string primitive)
    {
        var services = new ServiceCollection();
        Register(primitive, services, OnRedisUnavailable.Refuse);

        Assert.Throws<InvalidOperationException>(() => Register(primitive, services, OnRedisUnavailable.Grant));
    }

    [Fact]
    public void A_claim_and_a_single_flight_each_register_their_floor()
    {
        var services = new ServiceCollection();
        services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse);
        services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse);

        Assert.Equal(["claim", "single-flight"], services.Select(d => d.ImplementationInstance).OfType<RedisRequirement>().Select(r => r.Primitive).Order());
    }

    [Fact]
    public void A_slot_registers_its_floor_and_the_start_up_check()
    {
        var services = new ServiceCollection();
        services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);

        Assert.Equal("slot", Assert.Single(services.Select(d => d.ImplementationInstance).OfType<RedisRequirement>()).Primitive);
        Assert.Single(services, IsRedisStartupCheck);
    }

    [Fact]
    public void Both_policies_register_a_floor_for_a_claim()
    {
        foreach (var policy in Policies)
        {
            var services = new ServiceCollection();
            services.AddReeveClusterRedisClaims(policy);
            Assert.Single(services, d => d.ImplementationInstance is RedisRequirement);
        }
    }

    [Fact]
    public void Claims_only_and_single_flight_only_register_one_startup_check_and_the_umbrella_keeps_it_single()
    {
        foreach (var registerPrimitive in new Action<IServiceCollection>[]
                 {
                     services => services.AddReeveClusterRedisClaims(OnRedisUnavailable.Refuse),
                     services => services.AddReeveClusterRedisSingleFlight(OnRedisUnavailable.Refuse),
                 })
        {
            var services = new ServiceCollection();
            registerPrimitive(services);
            Assert.Single(services, IsRedisStartupCheck);

            services.AddReeveClusterRedis();
            Assert.Single(services, IsRedisStartupCheck);
        }
    }

    private static bool IsRedisStartupCheck(ServiceDescriptor descriptor) =>
        descriptor.ServiceType == typeof(IHostedService) && descriptor.ImplementationType == typeof(RedisStartupCheck);
}
