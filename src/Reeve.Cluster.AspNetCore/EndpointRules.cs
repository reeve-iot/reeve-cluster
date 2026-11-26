using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;

namespace Reeve.Cluster.AspNetCore;

internal static class EndpointRules
{
    private static readonly ConcurrentDictionary<Type, bool> ConnectionExempt = new();

    public static bool IsMarked(Endpoint? endpoint) => endpoint?.Metadata.GetMetadata<AllowWhenFencedAttribute>() is not null;

    public static bool IsAdmittedWhenNotServing(Endpoint? endpoint) =>
        IsMarked(endpoint) || (HubTypeOf(endpoint) is { } hub && IsConnectionExempt(hub));

    public static bool SkipsReadIndex(Endpoint? endpoint) => endpoint?.Metadata.GetMetadata<SkipReadIndexAttribute>() is not null;

    public static Type? HubTypeOf(Endpoint? endpoint) => endpoint?.Metadata.GetMetadata<HubMetadata>()?.HubType;

    public static bool IsConnectionExempt(Type hub) => ConnectionExempt.GetOrAdd(hub, IsHubExempt);

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Every hub type here is a THub that MapHub<THub> mapped, read from HubMetadata or from the activated hub; MapHub keeps the public methods of THub.")]
    private static bool IsHubExempt(Type hub) =>
        hub.IsDefined(typeof(AllowWhenFencedAttribute), inherit: true)
        || hub.GetMethods(BindingFlags.Public | BindingFlags.Instance).Any(m => m.IsDefined(typeof(AllowWhenFencedAttribute), inherit: true));

    public static bool IsInvocationExempt(Type hub, MethodInfo method) =>
        hub.IsDefined(typeof(AllowWhenFencedAttribute), inherit: true) || method.IsDefined(typeof(AllowWhenFencedAttribute), inherit: true);

    public static bool InvocationSkipsReadIndex(Type hub, MethodInfo method) =>
        hub.IsDefined(typeof(SkipReadIndexAttribute), inherit: true) || method.IsDefined(typeof(SkipReadIndexAttribute), inherit: true);
}
