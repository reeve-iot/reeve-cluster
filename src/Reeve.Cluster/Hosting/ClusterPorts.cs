using System.Diagnostics.CodeAnalysis;

namespace Reeve.Cluster;

/// <summary>
/// The application's ports to the library, each a required constructor parameter: there is no way to build the
/// registration without naming every one. A port an
/// application has nothing to put in is named explicitly — <see cref="NoFenceSink"/> — never left out.
/// </summary>
public sealed class ClusterPorts
{
    /// <summary>Names the application's ports.</summary>
    /// <param name="reconcilers">The reconciler types, in the order the application declares: the applier runs the dirty ones in it. May be empty for an application with no configuration. Each is registered as a singleton and resolved from the container, so it takes an <c>IDbContextFactory</c> or an <c>IServiceScopeFactory</c> for anything scoped, never a scoped service directly.</param>
    /// <param name="fenceSink">The type of the fence-step-4 sink, resolved from the container; <see cref="NoFenceSink"/> when no connection stays open past a request.</param>
    [RequiresUnreferencedCode("The container activates each reconciler type through its public constructors, which the trimmer cannot see through a list of types. Keep them, for example with DynamicDependency on the code that builds the ports, and suppress this warning there.")]
    public ClusterPorts(IReadOnlyList<Type> reconcilers, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type fenceSink)
    {
        ArgumentNullException.ThrowIfNull(reconcilers);
        ArgumentNullException.ThrowIfNull(fenceSink);

        foreach (var type in reconcilers)
        {
            Require(type, typeof(IReconciler), nameof(reconcilers));
        }

        Require(fenceSink, typeof(IFenceSink), nameof(fenceSink));

        Reconcilers = [.. reconcilers];
        FenceSink = fenceSink;
    }

    internal IReadOnlyList<Type> Reconcilers { get; }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    internal Type FenceSink { get; }

    private static void Require(Type type, Type port, string parameter)
    {
        ArgumentNullException.ThrowIfNull(type, parameter);
        if (type.IsAbstract || type.IsInterface || !port.IsAssignableFrom(type))
        {
            throw new ArgumentException($"{type} must be a concrete type that implements {port.Name}.", parameter);
        }
    }
}
