using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Migrations;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>The migration runner's registrations.</summary>
public static class ClusterMigrationsServiceCollectionExtensions
{
    /// <summary>
    /// The entry point of a migration-only run: registers the migration runner and the options it reads, and nothing that
    /// needs a lease or Redis — in particular no <see cref="IChangeAck"/>, so resolving one in this container fails.
    /// An application that also calls <c>AddReeveCluster</c> gets the runner from that call too; the
    /// connection string then comes from <see cref="ClusterOptions.ConnectionString"/> unless this method is called after it.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets <see cref="ClusterMigrationOptions.ConnectionString"/>.</param>
    public static IServiceCollection AddReeveClusterMigrations(this IServiceCollection services, Action<ClusterMigrationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<ClusterMigrationOptions>().Configure(configure);
        return services.AddClusterMigrations();
    }

    internal static IServiceCollection AddClusterMigrations(this IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<ClusterMigrationOptions>, ClusterMigrationOptionsValidation>());
        services.TryAddSingleton<IClusterMigrationRunner>(sp => new ClusterMigrationRunner(
            sp.GetRequiredService<IOptions<ClusterMigrationOptions>>(),
            sp.GetService<ILogger<ClusterMigrationRunner>>() ?? NullLogger<ClusterMigrationRunner>.Instance));
        return services;
    }

    internal static IServiceCollection AddClusterMigrationsFromClusterOptions(this IServiceCollection services)
    {
        services.AddOptions<ClusterMigrationOptions>()
            .Configure<IOptions<ClusterOptions>>((migration, cluster) => migration.ConnectionString = cluster.Value.ConnectionString);
        return services.AddClusterMigrations();
    }
}

internal sealed class ClusterMigrationOptionsValidation : IValidateOptions<ClusterMigrationOptions>
{
    public ValidateOptionsResult Validate(string? name, ClusterMigrationOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail("ClusterMigrationOptions.ConnectionString is required: the primary of the database that holds the reeve_cluster schema.");
        }

        var refusal = PrimaryTarget.Refusal(options.ConnectionString);
        return refusal is null
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail($"ClusterMigrationOptions.ConnectionString must name the primary: {refusal}.");
    }
}
