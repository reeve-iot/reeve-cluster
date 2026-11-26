using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the handlers of <c>Ask</c>.</summary>
public static class ClusterAskServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="THandler"/> as the handler of the requests <see cref="IClusterAsk.AskAsync"/> delivers to the
    /// owner of a key of <paramref name="kind"/>. The handler is a singleton resolved from the container; it takes an
    /// <c>IServiceScopeFactory</c> for anything scoped. A replica that does not run the kind's keys never takes the request.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="kind">The kind of the owned keys the handler answers for.</param>
    /// <exception cref="ArgumentException">A handler is registered for the kind already.</exception>
    public static IServiceCollection AddClusterAskHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(this IServiceCollection services, string kind)
        where THandler : class, IAskHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        ClusterCapabilities.Validate(kind);
        if (services.Any(d => d.ImplementationInstance is AskHandlerRegistration { Kind: { } registered } && registered == kind))
        {
            throw new ArgumentException($"A handler for the kind '{kind}' is registered already.", nameof(kind));
        }

        services.TryAddSingleton<THandler>();
        services.AddSingleton(new AskHandlerRegistration(kind, typeof(THandler)));
        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="THandler"/> as the handler of the requests <see cref="IClusterAsk.AskMemberAsync(string, ReadOnlyMemory{byte}, TimeSpan, CancellationToken)"/> delivers to
    /// this replica, which takes them in any phase, a fenced one included. The handler is a singleton resolved from the container.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <exception cref="ArgumentException">A member handler is registered already.</exception>
    public static IServiceCollection AddClusterMemberAskHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(this IServiceCollection services)
        where THandler : class, IAskHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(d => d.ImplementationInstance is AskHandlerRegistration { Kind: null }))
        {
            throw new ArgumentException("A member handler is registered already.", nameof(services));
        }

        services.TryAddSingleton<THandler>();
        services.AddSingleton(new AskHandlerRegistration(null, typeof(THandler)));
        return services;
    }
}

internal sealed record AskHandlerRegistration(string? Kind, Type HandlerType);

internal static class AskServiceCollectionExtensions
{
    public static IServiceCollection AddClusterAsk(this IServiceCollection services)
    {
        services.TryAddSingleton<AskStore>(sp => new AskStore(sp.GetRequiredService<ClusterDataSource>()));
        services.TryAddSingleton<IAskDirectory>(sp => sp.GetRequiredService<AskStore>());

        services.TryAddSingleton<IRunningTenures>(sp => sp.GetRequiredService<UnitRuntime>());

        services.TryAddSingleton(sp => new AskHandlers(() =>
        {
            var registrations = sp.GetServices<AskHandlerRegistration>().ToList();
            var keyed = registrations
                .Where(r => r.Kind is not null)
                .ToDictionary(r => r.Kind!, r => (IAskHandler)sp.GetRequiredService(r.HandlerType), StringComparer.Ordinal);
            var member = registrations.FirstOrDefault(r => r.Kind is null) is { } m ? (IAskHandler)sp.GetRequiredService(m.HandlerType) : null;
            return ((IReadOnlyDictionary<string, IAskHandler>)keyed, member);
        }));
        services.TryAddSingleton<AskInbound>(sp => new AskInbound(
            sp.GetRequiredService<IPhaseGate>(),
            sp.GetRequiredService<IClusterScopeSource>(),
            sp.GetRequiredService<IRunningTenures>(),
            sp.GetRequiredService<AskHandlers>(),
            sp.GetRequiredService<ILogger<AskInbound>>()));
        services.TryAddSingleton<IAskInbound>(sp => sp.GetRequiredService<AskInbound>());

        services.TryAddSingleton<IAskTransport, NoAskTransport>();
        services.TryAddSingleton<ClusterAsk>();
        services.TryAddSingleton<IClusterAsk>(sp => sp.GetRequiredService<ClusterAsk>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, AskStartup>());
        return services;
    }
}

internal sealed class AskStartup : IHostedService
{
    public AskStartup(AskInbound inbound, ClusterAsk ask, AskHandlers handlers)
    {
        _ = (inbound, ask);
        handlers.Resolve();
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
