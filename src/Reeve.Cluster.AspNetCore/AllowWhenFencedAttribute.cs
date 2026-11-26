namespace Reeve.Cluster.AspNetCore;

/// <summary>
/// Marks an endpoint that a replica serves while it is not <c>Serving</c> — in <c>Starting</c> after the first load,
/// <c>CatchingUp</c>, <c>Activated</c>, <c>Draining</c>, <c>Fencing</c> and <c>Fenced</c> — as well as while it is. The typical set is sign-in, health, metrics, version and status endpoints, static assets and the skip
/// endpoint. Its writes commit in every phase and the acknowledgement filter does not await them; on a replica that is not
/// serving it runs without the read index, and on a <c>Serving</c> one it waits on it like any request unless its endpoint
/// group is marked <see cref="SkipReadIndexAttribute"/>.
/// </summary>
/// <remarks>
/// On an MVC controller or action and on a minimal API endpoint it marks the endpoint. On a hub, on the class it marks the
/// connection and every method; on a method, that invocation only. A replica that is not serving accepts a hub connection
/// when the hub class or any of its methods is marked, and answers a non-exempt invocation on that connection as it answers a
/// non-exempt request.
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class AllowWhenFencedAttribute : Attribute
{
}

/// <summary>
/// Turns the read index's wait off for an endpoint group — a controller, a hub, a hub method, or the endpoints of a
/// <c>MapGroup</c> through <see cref="Microsoft.AspNetCore.Builder.ReeveClusterEndpointExtensions.SkipReadIndex{TBuilder}"/> — where a linearizable read is not needed
/// (static assets, health, metrics). The request still pins the current generation; it only skips the wait.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class SkipReadIndexAttribute : Attribute
{
}
