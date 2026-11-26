using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Hosting;

namespace Unit.Migrations;

public sealed class AddReeveClusterMigrationsTests
{
    private const string ConnectionString = "Host=db;Database=app";

    private static ServiceProvider Build() =>
        new ServiceCollection().AddReeveClusterMigrations(o => o.ConnectionString = ConnectionString).BuildServiceProvider();

    [Fact]
    public void A_container_built_with_the_migration_entry_point_alone_resolves_the_runner()
    {
        using var provider = Build();

        Assert.NotNull(provider.GetRequiredService<IClusterMigrationRunner>());
    }

    [Fact]
    public void A_container_built_with_the_migration_entry_point_alone_fails_to_resolve_IChangeAck()
    {
        using var provider = Build();

        Assert.Null(provider.GetService<IChangeAck>());
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IChangeAck>());
    }

    [Fact]
    public void The_migration_entry_point_registers_no_hosted_service_and_no_options_of_the_lease()
    {
        var services = new ServiceCollection().AddReeveClusterMigrations(o => o.ConnectionString = ConnectionString);

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IHostedService));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IOptions<ClusterOptions>));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IChangeLog));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IChangeAck));
    }

    [Fact]
    public void The_full_library_registers_the_runner_too_and_it_takes_the_connection_string_of_the_cluster_options()
    {
        var services = new ServiceCollection().AddReeveCluster(
            new ClusterPorts([], typeof(NoFenceSink)),
            o =>
            {
                o.ReplicaId = "r1";
                o.ConnectionString = ConnectionString;
                o.ReconcileTimeout = TimeSpan.FromSeconds(10);
            });
        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IClusterMigrationRunner>());
        Assert.Equal(ConnectionString, provider.GetRequiredService<IOptions<ClusterMigrationOptions>>().Value.ConnectionString);
    }

    [Fact]
    public void A_missing_connection_string_is_refused_when_the_options_are_read()
    {
        using var provider = new ServiceCollection().AddReeveClusterMigrations(_ => { }).BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterMigrationOptions>>().Value);
    }

    [Fact]
    public void I24_a_connection_string_that_may_reach_a_standby_is_refused_when_the_options_are_read()
    {
        using var provider = new ServiceCollection()
            .AddReeveClusterMigrations(o => o.ConnectionString = "Host=a,b;Database=app;Target Session Attributes=any")
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<ClusterMigrationOptions>>().Value);
    }
}
