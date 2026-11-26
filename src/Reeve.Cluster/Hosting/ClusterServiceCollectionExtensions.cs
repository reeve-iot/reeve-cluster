using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Slots;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the library.</summary>
public static class ClusterServiceCollectionExtensions
{
    /// <summary>
    /// Registers the cluster library with every area (<see cref="ClusterAreas.All"/>): its options, validated at start-up, the
    /// application's <paramref name="ports"/>, and each area's services through the area's own extension. Registers nothing
    /// that needs Redis, EF Core or ASP.NET Core; the packages for those add their own.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="ports">The application's ports.</param>
    /// <param name="configure">Sets the options; <c>ReplicaId</c> and <c>ConnectionString</c> are required, and so is
    /// <c>ReconcileTimeout</c> when <paramref name="ports"/> names a reconciler (10 seconds by default when it names none).</param>
    public static IServiceCollection AddReeveCluster(this IServiceCollection services, ClusterPorts ports, Action<ClusterOptions> configure) =>
        AddReeveCluster(services, ports, ClusterAreas.All, configure);

    /// <summary>
    /// Registers the cluster library with the optional <paramref name="areas"/>: its options, validated at start-up, the
    /// application's <paramref name="ports"/>, membership, the change log, the read index and the migrations, and each
    /// area that is on. <see cref="ClusterAreas.Primitives"/>, <see cref="ClusterAreas.Ask"/> and
    /// <see cref="ClusterAreas.Commands"/> bring <see cref="ClusterAreas.OwnedWork"/> with them. The start is refused with a
    /// message naming the area when a unit kind, an <c>Ask</c> handler, a durable command handler, a once effect or a
    /// Redis primitive is registered and its area is off; resolving a service of an area that is off (<c>IClusterClaims</c>,
    /// <c>IClusterAsk</c>, <c>IClusterCommands</c> and the like) is a dependency-injection error. With
    /// <see cref="ClusterAreas.Ask"/> off, the Redis package registers no <c>Ask</c> relay and subscribes no <c>Ask</c> relay channel.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="ports">The application's ports.</param>
    /// <param name="areas">The optional areas to register; <see cref="ClusterAreas.All"/> is what the overload without areas registers.</param>
    /// <param name="configure">Sets the options; <c>ReplicaId</c> and <c>ConnectionString</c> are required, and so is
    /// <c>ReconcileTimeout</c> when <paramref name="ports"/> names a reconciler (10 seconds by default when it names none).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="areas"/> has a flag that is not an area.</exception>
    public static IServiceCollection AddReeveCluster(this IServiceCollection services, ClusterPorts ports, ClusterAreas areas, Action<ClusterOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(ports);
        ArgumentNullException.ThrowIfNull(configure);
        if ((areas & ~ClusterAreas.All) != ClusterAreas.None)
        {
            throw new ArgumentOutOfRangeException(nameof(areas), areas, "Not a combination of ClusterAreas.");
        }

        var selection = ClusterAreaSelection.Of(areas);

        services.TryAddSingleton(TimeProvider.System);

        var options = services.AddOptions<ClusterOptions>().Configure(configure);

        if (ports.Reconcilers.Count == 0)
        {
            options.PostConfigure(o => o.ReconcileTimeout ??= TimeSpan.FromSeconds(10));
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<ClusterOptions>, ClusterOptionsValidation>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<ClusterOptions>, ClusterAreaValidation>());
        services.TryAddSingleton(selection);

        RegisterPorts(services, ports);

        services
            .AddClusterMigrationsFromClusterOptions()
            .AddClusterMembership()
            .AddClusterLog();
        if (selection.Has(ClusterAreas.OwnedWork))
        {
            services.AddClusterOwned();
        }

        if (selection.Has(ClusterAreas.Ask))
        {
            services.AddClusterAsk();
        }
        else
        {
            foreach (var registered in services.Where(d => !d.IsKeyedService && d.ImplementationInstance is AskAreaDescriptors).ToList())
            {
                foreach (var descriptor in ((AskAreaDescriptors)registered.ImplementationInstance!).Descriptors)
                {
                    services.Remove(descriptor);
                }

                services.Remove(registered);
            }
        }

        if (selection.Has(ClusterAreas.Commands))
        {
            services.AddClusterCommands();
        }

        if (selection.Has(ClusterAreas.Primitives))
        {
            services.AddClusterClaims().AddClusterSlots();
        }

        return services;
    }

    /// <summary>
    /// Registers the unit kind <paramref name="kind"/>, run by <typeparamref name="TUnit"/>: the
    /// replica advertises <c>unit:&lt;kind&gt;</c> and runs the keys of the kind it is assigned while it serves. The unit is a
    /// singleton resolved from the container; it takes an <c>IServiceScopeFactory</c> for anything scoped. A kind registered
    /// twice is refused, and so is the name <c>placer</c>, the library's own, a convergent kind with
    /// <see cref="UnitKindOptions.RedisFence"/>, and a policy other than <see cref="AdoptionPolicy.Placer"/> (only the placer
    /// inserts a unit's row; a kind that is adopted or waits for its creator is a keyed resource, see
    /// <see cref="AddClusterKeyedResource{TUnit}"/>). The first kind registered here (the free-row sweep is one) brings the placer.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="kind">The kind's name, a capability name (<see cref="ClusterCapabilities.Validate"/>).</param>
    /// <param name="configure">Sets the kind's declaration; the defaults are those of <see cref="UnitKindOptions"/>.</param>
    /// <exception cref="ArgumentException">The policy is <see cref="AdoptionPolicy.AnyEligible"/> or <see cref="AdoptionPolicy.None"/>, the kind is registered already, it is <c>placer</c>, or it is convergent with <see cref="UnitKindOptions.RedisFence"/>.</exception>
    public static IServiceCollection AddClusterUnit<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TUnit>(this IServiceCollection services, string kind, Action<UnitKindOptions>? configure = null)
        where TUnit : class, IOwnedUnit =>
        AddKind<TUnit>(services, kind, configure, keyed: false);

    /// <summary>
    /// Registers the keyed-resource kind <paramref name="kind"/>, run by <typeparamref name="TUnit"/>:
    /// as <see cref="AddClusterUnit{TUnit}"/>, but the kind's keys are created and ended by the application —
    /// <see cref="IKeyedResources.CreateAsync"/>, <see cref="OwnedContext.EndAsync"/> — and the placer never inserts one.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="kind">The kind's name, a capability name (<see cref="ClusterCapabilities.Validate"/>).</param>
    /// <param name="configure">Sets the kind's declaration, whose <see cref="UnitKindOptions.Policy"/> is
    /// <see cref="AdoptionPolicy.AnyEligible"/> or <see cref="AdoptionPolicy.None"/>: a free keyed row may be an ended resource's
    /// tombstone, which the placer cannot tell from a released row. The other defaults are those of <see cref="UnitKindOptions"/>.</param>
    /// <exception cref="ArgumentException">The policy is <see cref="AdoptionPolicy.Placer"/>, the kind is registered already, it is <c>placer</c>, or it is convergent with <see cref="UnitKindOptions.RedisFence"/>.</exception>
    public static IServiceCollection AddClusterKeyedResource<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TUnit>(this IServiceCollection services, string kind, Action<UnitKindOptions> configure)
        where TUnit : class, IOwnedUnit
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddKind<TUnit>(services, kind, configure, keyed: true);
    }

    private static IServiceCollection AddKind<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TUnit>(IServiceCollection services, string kind, Action<UnitKindOptions>? configure, bool keyed)
        where TUnit : class, IOwnedUnit
    {
        ArgumentNullException.ThrowIfNull(services);
        ClusterCapabilities.Validate(kind);
        var declaration = new UnitKindOptions();
        configure?.Invoke(declaration);
        ArgumentNullException.ThrowIfNull(declaration.Eligibility, nameof(configure));
        ArgumentOutOfRangeException.ThrowIfLessThan(declaration.StartTime, TimeSpan.Zero, nameof(configure));
        if (keyed && declaration.Policy == AdoptionPolicy.Placer)
        {
            throw new ArgumentException($"The keyed kind '{kind}' cannot have the Placer policy: set AnyEligible or None.", nameof(configure));
        }

        if (!keyed && declaration.Policy != AdoptionPolicy.Placer)
        {
            throw new ArgumentException(
                $"The unit kind '{kind}' cannot have the {declaration.Policy} policy: only the placer inserts a unit's row, so it would never run. Keep Placer, or register a keyed resource with AddClusterKeyedResource.",
                nameof(configure));
        }

        if (declaration.Convergent && declaration.RedisFence)
        {
            throw new ArgumentException($"The convergent kind '{kind}' cannot have RedisFence: a convergent unit has no token.", nameof(configure));
        }

        if (kind == Placement.PlacerKind)
        {
            throw new ArgumentException($"The unit kind '{kind}' is the library's own.", nameof(kind));
        }

        if (services.Any(d => d.ImplementationInstance is UnitKind registered && registered.Kind == kind))
        {
            throw new ArgumentException($"The unit kind '{kind}' is registered already.", nameof(kind));
        }

        services.TryAddSingleton<TUnit>();
        services.AddSingleton(new UnitKind(kind, typeof(TUnit), declaration) { Keyed = keyed });

        if (declaration.Policy == AdoptionPolicy.Placer
            && !services.Any(d => d.ImplementationInstance is UnitKind registered && registered.Kind == Placement.PlacerKind))
        {
            services.AddSingleton(new UnitKind(Placement.PlacerKind, typeof(PlacerUnit), new UnitKindOptions { Policy = AdoptionPolicy.None }));
        }

        return services;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "The public ClusterPorts constructor that fills Reconcilers requires unreferenced code, so the application is warned there.")]
    private static void RegisterPorts(IServiceCollection services, ClusterPorts ports)
    {
        foreach (var reconciler in ports.Reconcilers)
        {
            services.AddSingleton(typeof(IReconciler), reconciler);
        }

        services.AddSingleton(typeof(IFenceSink), ports.FenceSink);
    }
}
