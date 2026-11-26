using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the handlers of durable commands.</summary>
public static class ClusterCommandServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="THandler"/> as the handler of the durable commands sent to keys of <paramref name="kind"/>, with
    /// the kind's in-doubt policy. The kind must be registered on this replica as a non-convergent unit or a keyed resource, since
    /// a command is taken under the key's token; a replica that does not run the kind's keys never takes its commands. The handler
    /// is a singleton resolved from the container; it takes an <c>IServiceScopeFactory</c> for anything scoped.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="kind">The kind of the owned keys the handler serves.</param>
    /// <param name="inDoubt">What the next owner does with a command taken by a tenure that ended before it completed.</param>
    /// <exception cref="ArgumentException">A handler is registered for the kind already, or the policy is not one of the two.</exception>
    public static IServiceCollection AddClusterCommandHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(this IServiceCollection services, string kind, InDoubtPolicy inDoubt)
        where THandler : class, IDurableCommandHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        ClusterCapabilities.Validate(kind);
        if (!Enum.IsDefined(inDoubt))
        {
            throw new ArgumentException($"'{inDoubt}' is not an in-doubt policy: use ReRun or Complete.", nameof(inDoubt));
        }

        if (services.Any(d => d.ImplementationInstance is CommandHandlerRegistration registered && registered.Kind == kind))
        {
            throw new ArgumentException($"A durable command handler for the kind '{kind}' is registered already.", nameof(kind));
        }

        services.TryAddSingleton<THandler>();
        services.AddSingleton(new CommandHandlerRegistration(kind, typeof(THandler), inDoubt));
        return services;
    }
}

internal sealed record CommandHandlerRegistration(string Kind, Type HandlerType, InDoubtPolicy InDoubt);

internal static class CommandServiceCollectionExtensions
{
    public static IServiceCollection AddClusterCommands(this IServiceCollection services)
    {
        services.TryAddSingleton<CommandStore>(sp => new CommandStore(sp.GetRequiredService<OwnedDataSource>(), sp.GetRequiredService<IOptions<ClusterOptions>>()));
        services.TryAddSingleton<ICommandRows>(sp => sp.GetRequiredService<CommandStore>());
        services.TryAddSingleton<ICommandTenures>(sp => sp.GetRequiredService<UnitRuntime>());
        services.TryAddSingleton<CommandHandlers>(BuildHandlers);
        services.TryAddSingleton<ClusterCommands>(sp => new ClusterCommands(
            sp.GetRequiredService<ICommandRows>(),
            sp.GetServices<CommandHandlerRegistration>(),
            sp.GetRequiredService<IWakeupChannel>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<ClusterCommands>>()));
        services.TryAddSingleton<IClusterCommands>(sp => sp.GetRequiredService<ClusterCommands>());
        services.TryAddSingleton<CommandPickup>(sp => new CommandPickup(
            sp.GetRequiredService<ICommandRows>(),
            sp.GetRequiredService<ICommandTenures>(),
            sp.GetRequiredService<CommandHandlers>(),
            sp.GetServices<UnitKind>(),
            sp.GetRequiredService<IClusterScopeSource>(),
            sp.GetRequiredService<IFaultPoints>(),
            sp.GetRequiredService<IWakeupChannel>(),
            sp.GetRequiredService<IHostApplicationLifetime>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<CommandPickup>>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, CommandPickup>(sp => sp.GetRequiredService<CommandPickup>()));

        services.TryAddSingleton<CommandSweep>(sp => new CommandSweep(
            sp.GetRequiredService<ICommandRows>(),
            sp.GetRequiredService<IWakeupChannel>(),
            sp.GetRequiredService<IOptions<ClusterOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<CommandSweep>>()));
        if (!services.Any(d => d.ImplementationInstance is UnitKind { Kind: CommandSweep.Kind }))
        {
            services.AddClusterUnit<CommandSweep>(CommandSweep.Kind, o => o.Convergent = true);
        }

        return services;
    }

    private static CommandHandlers BuildHandlers(IServiceProvider sp) => new(
        sp.GetServices<CommandHandlerRegistration>()
            .ToDictionary(r => r.Kind, r => (Handler: (IDurableCommandHandler)sp.GetRequiredService(r.HandlerType), r.InDoubt), StringComparer.Ordinal));
}
