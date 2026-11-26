using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Reeve.Cluster;
using Reeve.Cluster.Status;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers an application's status sections.</summary>
public static class ClusterStatusServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="T"/> as a singleton source of an application status section: the library collects it every
    /// <c>StatusReportInterval</c> and writes the section beside its own, and it appears in <c>IClusterStatus</c> and in the status endpoint's body
    /// as they do. May be called before or after <c>AddReeveCluster</c>. <typeparamref name="T"/> is registered once (<c>TryAddSingleton</c>) and the
    /// registry is pointed at that instance, so a type the application also registers itself (as a worker that holds the state it reports) is one object.
    /// </summary>
    /// <remarks>
    /// The sources are collected one after the other, each bounded by <c>StatusReportInterval</c>: a source must return quickly and read its own memory.
    /// A source that does not return is dropped for each tick, and its abandoned collects are not cancelled by the library.
    /// </remarks>
    /// <typeparam name="T">The source. Its <see cref="IStatusSectionSource.Name"/> must be unique among the registered sources and none of the library's own section names;
    /// the host's start is refused with a <see cref="ClusterConfigurationException"/> (<see cref="ClusterConfigurationReason.StatusSectionNameDuplicate"/>,
    /// <see cref="ClusterConfigurationReason.StatusSectionNameReserved"/> or <see cref="ClusterConfigurationReason.StatusSectionNameInvalid"/>) otherwise, so registering one type twice is refused too.</typeparam>
    /// <param name="services">The service collection.</param>
    public static IServiceCollection AddClusterStatusSection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(this IServiceCollection services)
        where T : class, IStatusSectionSource
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<T>();
        services.AddSingleton<IStatusSectionSource>(sp => sp.GetRequiredService<T>());
        services.AddSingleton(new ApplicationStatusSection(typeof(T)));
        return services;
    }
}
