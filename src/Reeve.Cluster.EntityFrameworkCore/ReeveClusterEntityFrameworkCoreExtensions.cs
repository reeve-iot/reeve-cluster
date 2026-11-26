using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the EF Core package.</summary>
public static class ReeveClusterEntityFrameworkCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the capture: the application's <paramref name="mapping"/>, built here so that an incomplete one is refused at
    /// start-up, the interceptor that <see cref="ReeveClusterEntityFrameworkCoreOptionsExtensions.UseReeveClusterCapture"/>
    /// adds to a context, and a start check for contexts registered through <c>DbContextOptions&lt;T&gt;</c>. The check refuses
    /// contexts it cannot build and mapped contexts without capture. Call it after <c>AddReeveCluster</c>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="mapping">Declares the configuration entity types, their aggregate types, keys and counted columns.</param>
    /// <exception cref="InvalidOperationException">The mapping is incomplete: a type without a key choice or a columns choice.</exception>
    public static IServiceCollection AddReeveClusterEntityFrameworkCore(this IServiceCollection services, Action<IClusterMappingBuilder> mapping)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(mapping);

        services.AddSingleton<IClusterMapping>(ClusterMapping.Build(mapping));
        if (!services.Any(d => !d.IsKeyedService && d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(AggregateReconcilerCheck)))
        {
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService, AggregateReconcilerCheck>());
        }
        services.TryAddSingleton<ClusterCaptureInterceptor>();
        services.TryAddSingleton(new DbContextServiceCollection(services));
        AddStartupCheck(services);
        return services;
    }

    private static void AddStartupCheck(IServiceCollection services)
    {
        if (!services.Any(descriptor => descriptor is { IsKeyedService: false }
            && descriptor.ServiceType == typeof(IHostedService)
            && descriptor.ImplementationType == typeof(ContextCaptureStartupCheck)))
        {
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService, ContextCaptureStartupCheck>());
        }
    }
}

/// <summary>Adds the capture to a context.</summary>
public static class ReeveClusterEntityFrameworkCoreOptionsExtensions
{
    /// <summary>
    /// Adds the capture and bulk-operation guard interceptors to the context's options: <c>services.AddDbContext&lt;Db&gt;((sp, o) =&gt; o.UseNpgsql(...).UseReeveClusterCapture(sp))</c>.
    /// The interceptor sets <see cref="AutoTransactionBehavior.Always"/> for a save that captures and for every save in a
    /// non-exempt client request's scope, and leaves it set; other saves keep the context's setting.
    /// </summary>
    /// <param name="options">The context's options builder.</param>
    /// <param name="services">The application's service provider, which holds the interceptor.</param>
    public static DbContextOptionsBuilder UseReeveClusterCapture(this DbContextOptionsBuilder options, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(services);
        return options.AddClusterCapture(services.GetRequiredService<ClusterCaptureInterceptor>(), services.GetRequiredService<IClusterMapping>());
    }

    internal static DbContextOptionsBuilder AddClusterCapture(this DbContextOptionsBuilder options, ClusterCaptureInterceptor interceptor, IClusterMapping mapping)
    {
        ((IDbContextOptionsBuilderInfrastructure)options).AddOrUpdateExtension(new ClusterCaptureOptionsExtension(mapping));
        return options.AddInterceptors(interceptor, BulkOperationGuard.Instance);
    }
}
