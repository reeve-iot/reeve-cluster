using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Hosting;

namespace Unit.Hosting;

public sealed class AddReeveClusterTests
{
    private sealed class FirstReconciler : IReconciler
    {
        public string AggregateType => "first";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("first"));
    }

    private sealed class SecondReconciler : IReconciler
    {
        public string AggregateType => "second";

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("second"));
    }

    private sealed class DependsOnLater : IReconciler
    {
        public string AggregateType => "first";

        IReadOnlyList<string> IReconciler.DependsOn => ["second"];

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("first"));
    }

    private sealed class DependsOnGhost : IReconciler
    {
        public string AggregateType => "second";

        IReadOnlyList<string> IReconciler.DependsOn => ["ghost"];

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("second"));
    }

    private sealed class DependsOnFirst : IReconciler
    {
        public string AggregateType => "second";

        IReadOnlyList<string> IReconciler.DependsOn => ["first"];

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult("second"));
    }

    private sealed class StubLifetime : Microsoft.Extensions.Hosting.IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }

    private abstract class AbstractReconciler : IReconciler
    {
        public abstract string AggregateType { get; }

        public abstract Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken);
    }

    private static void Required(ClusterOptions o)
    {
        o.ReplicaId = "replica-1";
        o.ConnectionString = "Host=db;Database=app";
        o.ReconcileTimeout = TimeSpan.FromSeconds(10);
    }

    private static void ValidateAtStartup(IServiceProvider provider)
    {
        provider.GetRequiredService<IAsyncStartupValidator>().ValidateAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    private static ClusterPorts Ports() => new([typeof(FirstReconciler), typeof(SecondReconciler)], typeof(NoFenceSink));

    [Fact]
    public void The_ports_are_required_constructor_parameters()
    {
        Assert.Throws<ArgumentNullException>(() => new ClusterPorts(null!, typeof(NoFenceSink)));
        Assert.Throws<ArgumentNullException>(() => new ClusterPorts([], null!));
    }

    [Theory]
    [InlineData(typeof(string))]
    [InlineData(typeof(AbstractReconciler))]
    [InlineData(typeof(IReconciler))]
    public void A_reconciler_port_must_be_a_concrete_IReconciler(Type type)
    {
        Assert.Throws<ArgumentException>(() => new ClusterPorts([type], typeof(NoFenceSink)));
    }

    [Fact]
    public void The_fence_sink_port_must_be_a_concrete_IFenceSink()
    {
        Assert.Throws<ArgumentException>(() => new ClusterPorts([], typeof(FirstReconciler)));
        Assert.Throws<ArgumentException>(() => new ClusterPorts([], typeof(IFenceSink)));
    }

    [Fact]
    public void The_registration_resolves_the_reconcilers_in_the_declared_order_and_the_sink()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        using var provider = services.BuildServiceProvider();

        Assert.Equal(["first", "second"], provider.GetServices<IReconciler>().Select(r => r.AggregateType));
        Assert.IsType<NoFenceSink>(provider.GetRequiredService<IFenceSink>());
        Assert.Same(TimeProvider.System, provider.GetRequiredService<TimeProvider>());
    }

    [Fact]
    public async Task AddReeveCluster_registers_the_held_slots_and_resolves_them_with_the_claims_services()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(Ports(), Required);
        await using var provider = services.BuildServiceProvider();

        var slots = provider.GetRequiredService<IClusterSlots>();

        Assert.IsType<Reeve.Cluster.Slots.ClusterSlots>(slots);
        Assert.Same(slots, provider.GetRequiredService<IClusterSlots>());
        Assert.NotNull(provider.GetRequiredService<IClusterClaims>());
    }

    [Fact]
    public async Task AddReeveCluster_registers_the_version_gate_the_floor_command_and_the_upgrade_path_guard()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(Ports(), Required);
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<Reeve.Cluster.Floors.ClusterVersionGate>(provider.GetRequiredService<IClusterVersionGate>());
        Assert.IsType<Reeve.Cluster.Floors.ClusterFloorCommand>(provider.GetRequiredService<IClusterFloorCommand>());
        var guard = provider.GetRequiredService<Reeve.Cluster.Floors.IUpgradePathGuard>();
        Assert.Equal(Reeve.Cluster.Membership.MemberIdentity.UpgradePathProtocol, guard.MinimumPeerProtocol);
        Assert.True(guard.MinimumPeerProtocol <= Reeve.Cluster.Membership.MemberIdentity.CurrentProtocol);
    }

    [Fact]
    public async Task AddReeveCluster_registers_the_typed_status_read_and_keeps_a_registration_of_the_host()
    {
        await using var own = new ServiceCollection().AddLogging().AddReeveCluster(Ports(), Required).BuildServiceProvider();
        Assert.IsType<Reeve.Cluster.Status.ClusterStatus>(own.GetRequiredService<IClusterStatus>());

        var hosts = new HostsStatus();
        await using var kept = new ServiceCollection().AddLogging().AddSingleton<IClusterStatus>(hosts).AddReeveCluster(Ports(), Required).BuildServiceProvider();
        Assert.Same(hosts, kept.GetRequiredService<IClusterStatus>());
    }

    private sealed class HostsStatus : IClusterStatus
    {
        public Task<ClusterStatusRead> ReadAsync(CancellationToken cancellationToken = default) => Task.FromResult(new ClusterStatusRead { ReadAt = DateTimeOffset.UnixEpoch, Members = [] });
    }

    [Theory]
    [InlineData(typeof(DependsOnLater), typeof(SecondReconciler), ClusterConfigurationReason.ReconcilerDependencyNotEarlier)]
    [InlineData(typeof(FirstReconciler), typeof(DependsOnGhost), ClusterConfigurationReason.ReconcilerDependencyUnknown)]
    public void The_host_refuses_an_invalid_DependsOn_before_any_hosted_service_starts(Type first, Type second, ClusterConfigurationReason reason)
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(new ClusterPorts([first, second], typeof(NoFenceSink)), Required);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostApplicationLifetime, StubLifetime>();
        using var provider = services.BuildServiceProvider();

        var thrown = Assert.ThrowsAny<Exception>(() => provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().ToList());

        var refusal = thrown as ClusterConfigurationException ?? thrown.InnerException as ClusterConfigurationException;
        Assert.NotNull(refusal);
        Assert.Equal(reason, refusal.Reason);
    }

    [Fact]
    public void The_store_of_a_valid_registration_owns_the_graph_of_its_DependsOn()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(new ClusterPorts([typeof(FirstReconciler), typeof(DependsOnFirst)], typeof(NoFenceSink)), Required);
        using var provider = services.BuildServiceProvider();

        var graph = provider.GetRequiredService<Reeve.Cluster.Log.GenerationStore>().Graph;

        Assert.Equal(["first", "second"], graph.Types);
        Assert.Equal(["first"], graph.DependenciesOf("second"));
    }

    [Fact]
    public void The_registration_resolves_the_readers_outside_a_request()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(Ports(), Required);
        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetRequiredService<IClusterGenerations>().Current);
        Assert.Equal(ReplicaPhase.Starting, provider.GetRequiredService<IReplicaPhase>().Current);
    }

    [Fact]
    public void An_application_with_no_configuration_names_an_empty_reconciler_list()
    {
        var services = new ServiceCollection().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        using var provider = services.BuildServiceProvider();

        Assert.Empty(provider.GetServices<IReconciler>());
    }

    [Fact]
    public void A_replica_with_a_missing_required_setting_refuses_to_start()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), o => o.ReconcileTimeout = TimeSpan.FromSeconds(10));
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterOptions>>().Value);

        Assert.Contains(ex.Failures, f => f.Contains("ReplicaId", StringComparison.Ordinal));
        Assert.Contains(ex.Failures, f => f.Contains("ConnectionString", StringComparison.Ordinal));
    }

    [Fact]
    public void Startup_validation_runs_when_the_host_validates_options_not_only_on_first_use()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), o => o.ReconcileTimeout = TimeSpan.FromSeconds(10));
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => ValidateAtStartup(provider));
    }

    [Fact]
    public void A_replica_with_invalid_timing_refuses_to_start()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), o =>
        {
            Required(o);
            o.StopBudget = TimeSpan.FromSeconds(1);
        });
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterOptions>>().Value);

        Assert.Contains(ex.Failures, f => f.Contains("StopBudget", StringComparison.Ordinal));
    }

    [Fact]
    public void With_AspNetCore_in_use_the_HTTP_settings_are_required()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        services.AddSingleton<AspNetCoreMarker>();
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterOptions>>().Value);

        Assert.Contains(ex.Failures, f => f.Contains("ProxyReadTimeout", StringComparison.Ordinal));
        Assert.Contains(ex.Failures, f => f.Contains("ActionTimeBudget", StringComparison.Ordinal));
    }

    [Fact]
    public void Valid_options_pass_and_carry_the_values_set()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<ClusterOptions>>().Value;

        Assert.Equal("replica-1", options.ReplicaId);
        Assert.Equal(TimeSpan.FromSeconds(36), options.AckTimeout);
    }

    [Fact]
    public async Task The_library_has_one_data_source_of_its_own()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        await using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<Reeve.Cluster.Core.ClusterDataSource>();
        var second = provider.GetRequiredService<Reeve.Cluster.Core.ClusterDataSource>();

        Assert.Same(first, second);
    }

    [Fact]
    public async Task The_registration_resolves_one_read_index()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>));
        await using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<Reeve.Cluster.Log.ReadIndex>();

        Assert.Same(first, provider.GetRequiredService<Reeve.Cluster.Log.ReadIndex>());
    }

    [Fact]
    public async Task The_read_index_reads_the_head_from_a_pool_of_its_own()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>));
        await using var provider = services.BuildServiceProvider();

        var source = provider.GetRequiredService<Reeve.Cluster.Log.ReadIndexDataSource>().Source;
        var readIndex = source.DataSource;
        var main = provider.GetRequiredService<Reeve.Cluster.Core.ClusterDataSource>().DataSource;

        Assert.Same(source, ((Reeve.Cluster.Log.ReadIndexStore)provider.GetRequiredService<Reeve.Cluster.Log.IReadIndexStore>()).Data);
        Assert.NotSame(main, readIndex);
        Assert.Equal(Reeve.Cluster.Log.ReadIndexDataSource.MaxPoolSize, new Npgsql.NpgsqlConnectionStringBuilder(readIndex.ConnectionString).MaxPoolSize);
        Assert.NotEqual(Reeve.Cluster.Log.ReadIndexDataSource.MaxPoolSize, new Npgsql.NpgsqlConnectionStringBuilder(main.ConnectionString).MaxPoolSize);
    }

    [Fact]
    public async Task The_ack_wait_reads_its_wait_set_from_the_read_index_pool_not_the_main_one()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), Required);
        services.AddSingleton(typeof(Microsoft.Extensions.Logging.ILogger<>), typeof(Microsoft.Extensions.Logging.Abstractions.NullLogger<>));
        await using var provider = services.BuildServiceProvider();

        var readIndex = provider.GetRequiredService<Reeve.Cluster.Log.ReadIndexDataSource>().Source;
        var ack = provider.GetRequiredService<Reeve.Cluster.Log.ChangeAck>();

        Assert.Same(readIndex, ack.Data);
        Assert.Same(ack, provider.GetRequiredService<Reeve.Cluster.IChangeAck>());
        Assert.NotSame(provider.GetRequiredService<Reeve.Cluster.Core.ClusterDataSource>(), ack.Data);
    }

    [Fact]
    public async Task The_read_index_pool_is_never_larger_than_the_connection_strings_own()
    {
        var services = new ServiceCollection().AddReeveCluster(Ports(), o =>
        {
            Required(o);
            o.ConnectionString = "Host=db;Database=app;Maximum Pool Size=4";
        });
        await using var provider = services.BuildServiceProvider();

        var readIndex = provider.GetRequiredService<Reeve.Cluster.Log.ReadIndexDataSource>().Source.DataSource;

        Assert.Equal(4, new Npgsql.NpgsqlConnectionStringBuilder(readIndex.ConnectionString).MaxPoolSize);
    }

    private sealed class JobUnit : IOwnedUnit
    {
        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => Task.CompletedTask;

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }

    private sealed class AskHandler : IAskHandler
    {
        public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private sealed class CommandHandler : IDurableCommandHandler
    {
        public Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken) => Task.FromResult<ReadOnlyMemory<byte>>(default);
    }

    private static readonly Type[] OwnedServices = [typeof(Reeve.Cluster.Owned.UnitRuntime), typeof(IKeyedResources)];
    private static readonly Type[] PrimitiveServices = [typeof(IClusterClaims), typeof(IClusterOnce), typeof(IClusterSlots), typeof(IClusterSingleFlight), typeof(IClusterBudget), typeof(IClusterCounter)];
    private static readonly Type[] AskServices = [typeof(IClusterAsk)];
    private static readonly Type[] CommandServices = [typeof(IClusterCommands)];

    private static ServiceCollection With(ClusterAreas areas, Action<ClusterOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), areas, configure ?? Required);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostApplicationLifetime, StubLifetime>();
        return services;
    }

    private static bool Registers(IServiceCollection services, Type[] area) => area.All(t => services.Any(d => d.ServiceType == t));

    private static bool RegistersNone(IServiceCollection services, Type[] area) => !area.Any(t => services.Any(d => d.ServiceType == t));

    [Fact]
    public async Task With_no_area_only_membership_and_the_log_are_registered_and_the_hosted_services_resolve()
    {
        var services = With(ClusterAreas.None);

        Assert.True(RegistersNone(services, [.. OwnedServices, .. PrimitiveServices, .. AskServices, .. CommandServices]));
        Assert.DoesNotContain(services, d => d.ImplementationInstance is Reeve.Cluster.Owned.UnitKind);
        await using var provider = services.BuildServiceProvider();
        Assert.NotEmpty(provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>());
        Assert.NotEmpty(provider.GetServices<IStatusSectionSource>());
        Assert.NotNull(provider.GetRequiredService<IReplicaPhase>());
        ValidateAtStartup(provider);
    }

    [Theory]
    [InlineData(ClusterAreas.OwnedWork)]
    [InlineData(ClusterAreas.Primitives)]
    [InlineData(ClusterAreas.Ask)]
    [InlineData(ClusterAreas.Commands)]
    public async Task Each_area_registers_itself_and_owned_work_and_no_other_area(ClusterAreas area)
    {
        var services = With(area);

        Assert.True(Registers(services, OwnedServices));
        Assert.Equal(area == ClusterAreas.Primitives, Registers(services, PrimitiveServices));
        Assert.Equal(area != ClusterAreas.Primitives, RegistersNone(services, PrimitiveServices));
        Assert.Equal(area == ClusterAreas.Ask, Registers(services, AskServices));
        Assert.Equal(area == ClusterAreas.Commands, Registers(services, CommandServices));
        await using var provider = services.BuildServiceProvider();
        Assert.NotEmpty(provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>());
        ValidateAtStartup(provider);
    }

    [Fact]
    public async Task The_overload_without_areas_keeps_the_hosted_service_order()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(Ports(), Required);
        services.AddSingleton<Microsoft.Extensions.Hosting.IHostApplicationLifetime, StubLifetime>();
        await using var provider = services.BuildServiceProvider();

        var order = provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>().Select(h => h.GetType().Name).ToList();

        Assert.Equal(ExpectedHostedServices, order);
        Assert.True(Registers(services, [.. OwnedServices, .. PrimitiveServices, .. AskServices, .. CommandServices]));
    }

    private static readonly string[] ExpectedHostedServices =
    [
        "FenceCoordinator", "StatusReporter", "ReplicationMonitor", "MembershipDriver", "SkipMonitor", "Retention", "UnitRuntime",
        "PlacerAdopter", "AskStartup", "CommandPickup",
    ];

    [Fact]
    public void An_area_that_is_not_one_is_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddReeveCluster(Ports(), (ClusterAreas)16, Required));
    }

    [Fact]
    public void Without_a_reconciler_ReconcileTimeout_defaults_to_ten_seconds_and_a_value_set_is_kept()
    {
        using var unset = With(ClusterAreas.All, o =>
        {
            o.ReplicaId = "replica-1";
            o.ConnectionString = "Host=db;Database=app";
        }).BuildServiceProvider();
        using var set = With(ClusterAreas.All, o =>
        {
            Required(o);
            o.ReconcileTimeout = TimeSpan.FromSeconds(3);
        }).BuildServiceProvider();

        Assert.Equal(TimeSpan.FromSeconds(10), unset.GetRequiredService<IOptions<ClusterOptions>>().Value.ReconcileTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), set.GetRequiredService<IOptions<ClusterOptions>>().Value.ReconcileTimeout);
    }

    [Fact]
    public void With_a_reconciler_ReconcileTimeout_is_still_required()
    {
        using var provider = new ServiceCollection().AddReeveCluster(Ports(), o =>
        {
            o.ReplicaId = "replica-1";
            o.ConnectionString = "Host=db;Database=app";
        }).BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterOptions>>().Value);

        Assert.Contains(ex.Failures, f => f.Contains("ReconcileTimeout", StringComparison.Ordinal));
    }

    public static TheoryData<string, ClusterAreas, string> Mismatches => new()
    {
        { "unit", ClusterAreas.None, "The unit kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.OwnedWork" },
        { "ask", ClusterAreas.OwnedWork | ClusterAreas.Commands, "The Ask handler of the kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.Ask" },
        { "member-ask", ClusterAreas.None, "The member Ask handler is registered, but AddReeveCluster was called without ClusterAreas.Ask" },
        { "command", ClusterAreas.Ask | ClusterAreas.Primitives, "The durable command handler of the kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.Commands" },
        { "once", ClusterAreas.Ask | ClusterAreas.Commands, "The once effect 'mail' is registered, but AddReeveCluster was called without ClusterAreas.Primitives" },
        { "redis", ClusterAreas.OwnedWork, "The Redis primitive IRedisCounter is registered, but AddReeveCluster was called without ClusterAreas.Primitives" },
    };

    [Theory]
    [MemberData(nameof(Mismatches))]
    public void A_registration_whose_area_is_off_refuses_the_start_and_names_the_area(string registration, ClusterAreas areas, string message)
    {
        var services = With(areas);
        _ = registration switch
        {
            "unit" => services.AddClusterUnit<JobUnit>("jobs"),
            "ask" => services.AddClusterAskHandler<AskHandler>("jobs"),
            "member-ask" => services.AddClusterMemberAskHandler<AskHandler>(),
            "command" => services.AddClusterCommandHandler<CommandHandler>("jobs", InDoubtPolicy.ReRun),
            "once" => services.AddClusterOnceEffect("mail", (_, _, _) => Task.CompletedTask),
            _ => services.AddReeveClusterRedisCounter(),
        };
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => ValidateAtStartup(provider));

        Assert.Single(ex.Failures, f => f.StartsWith(message, StringComparison.Ordinal));
    }

    [Fact]
    public void A_registration_made_before_AddReeveCluster_whose_area_is_off_refuses_the_start_too()
    {
        var services = new ServiceCollection();
        services.AddClusterUnit<JobUnit>("jobs")
            .AddClusterAskHandler<AskHandler>("jobs")
            .AddClusterCommandHandler<CommandHandler>("jobs", InDoubtPolicy.ReRun)
            .AddClusterOnceEffect("mail", (_, _, _) => Task.CompletedTask)
            .AddReeveClusterRedisClaims(Reeve.Cluster.Redis.OnRedisUnavailable.Refuse);
        services.AddLogging().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), ClusterAreas.None, Required);
        using var provider = services.BuildServiceProvider();

        var ex = Assert.Throws<OptionsValidationException>(() => ValidateAtStartup(provider));

        Assert.Single(ex.Failures, f => f.StartsWith("The unit kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.OwnedWork", StringComparison.Ordinal));
        Assert.Single(ex.Failures, f => f.StartsWith("The Ask handler of the kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.Ask", StringComparison.Ordinal));
        Assert.Single(ex.Failures, f => f.StartsWith("The durable command handler of the kind 'jobs' is registered, but AddReeveCluster was called without ClusterAreas.Commands", StringComparison.Ordinal));
        Assert.Single(ex.Failures, f => f.StartsWith("The once effect 'mail' is registered, but AddReeveCluster was called without ClusterAreas.Primitives", StringComparison.Ordinal));
        Assert.Single(ex.Failures, f => f.StartsWith("The Redis primitive IRedisClaims is registered, but AddReeveCluster was called without ClusterAreas.Primitives", StringComparison.Ordinal));
    }

    [Fact]
    public void Each_registration_with_its_area_on_starts()
    {
        var services = With(ClusterAreas.All);
        services.AddClusterUnit<JobUnit>("jobs")
            .AddClusterAskHandler<AskHandler>("jobs")
            .AddClusterCommandHandler<CommandHandler>("jobs", InDoubtPolicy.ReRun)
            .AddClusterOnceEffect("mail", (_, _, _) => Task.CompletedTask)
            .AddReeveClusterRedisCounter();
        using var provider = services.BuildServiceProvider();

        ValidateAtStartup(provider);
    }

    [Fact]
    public void A_reconcile_context_can_be_built_to_test_a_reconciler()
    {
        var context = new ReconcileContext(
            "first", head: 7, passNumber: 1, attempt: 1, fullLoad: false, hints: new HashSet<string> { "k" }, previous: null,
            transaction: new FakeTransaction(), openSnapshotSession: _ => throw new NotSupportedException());

        Assert.Equal("first", context.AggregateType);
        Assert.Equal(7, context.Head);
        Assert.Contains("k", context.Hints!);
        Assert.Throws<NotSupportedException>(() => context.OpenSnapshotSessionAsync().GetAwaiter().GetResult());
        Assert.Throws<InvalidOperationException>(() => context.Connection);
    }

    private sealed class FakeTransaction : DbTransaction
    {
        public override System.Data.IsolationLevel IsolationLevel => System.Data.IsolationLevel.RepeatableRead;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}
