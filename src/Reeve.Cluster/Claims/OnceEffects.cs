using Microsoft.Extensions.DependencyInjection.Extensions;
using Reeve.Cluster.Claims;

namespace Reeve.Cluster.Claims
{
    internal sealed record OnceEffectRegistration(string Name, Func<IServiceProvider, string, CancellationToken, Task> Effect);

    internal sealed class OnceEffects
    {
        private readonly Dictionary<string, Func<IServiceProvider, string, CancellationToken, Task>> effects = new(StringComparer.Ordinal);

        public OnceEffects(IEnumerable<OnceEffectRegistration> registrations)
        {
            ArgumentNullException.ThrowIfNull(registrations);
            foreach (var registration in registrations)
            {
                if (!effects.TryAdd(registration.Name, registration.Effect))
                {
                    throw new ArgumentException($"The once effect '{registration.Name}' is registered twice.", nameof(registrations));
                }
            }
        }

        public IReadOnlyCollection<string> Names => effects.Keys;

        public bool TryGet(string name, out Func<IServiceProvider, string, CancellationToken, Task> effect) => effects.TryGetValue(name, out effect!);
    }
}

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>Registers the effects of once-runs.</summary>
    public static class OnceServiceCollectionExtensions
    {
        /// <summary>
        /// Registers the effect <paramref name="name"/> of <see cref="Reeve.Cluster.IClusterOnce"/> on this replica. Every replica registers it: the
        /// first run is the caller's, the repeat of an <see cref="Reeve.Cluster.OnceMode.AtLeastOnce"/> run is the sweeper's, on whichever replica holds it.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="name">The effect's name.</param>
        /// <param name="effect">Receives a per-run service provider, the key and a token. The repeat runs without a generation.</param>
        /// <exception cref="ArgumentException">The name is registered already.</exception>
        public static IServiceCollection AddClusterOnceEffect(this IServiceCollection services, string name, Func<IServiceProvider, string, CancellationToken, Task> effect)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentException.ThrowIfNullOrEmpty(name);
            ArgumentNullException.ThrowIfNull(effect);
            if (services.Any(d => d.ImplementationInstance is OnceEffectRegistration registered && registered.Name == name))
            {
                throw new ArgumentException($"The once effect '{name}' is registered already.", nameof(name));
            }

            services.AddSingleton(new OnceEffectRegistration(name, effect));
            return services;
        }
    }
}
