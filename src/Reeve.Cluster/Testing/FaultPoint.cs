using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Reeve.Cluster;

internal readonly record struct FaultPoint(string Name, string? Action)
{
    public static FaultPoint At(string name, string action) => new(name, action);

    public static FaultPoint Unmodelled(string name) => new(name, null);

    public override string ToString() => Name;
}

internal static class FaultPointsExtensions
{
    public static ValueTask ReachAsync(this IFaultPoints points, FaultPoint point, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(points);
        return points.ReachAsync(point.Name, cancellationToken);
    }
}

internal static class FaultPointCatalogue
{
    public static IReadOnlyList<Type> Areas { get; } =
    [
        Area(typeof(MembershipFaultPoints)),
        Area(typeof(LogFaultPoints)),
        Area(typeof(CaptureFaultPoints)),
        Area(typeof(HttpFaultPoints)),
        Area(typeof(OwnedFaultPoints)),
        Area(typeof(ClaimsFaultPoints)),
        Area(typeof(SlotsFaultPoints)),
        Area(typeof(CommandsFaultPoints)),
    ];

    public static IReadOnlyList<FaultPoint> All { get; } = [.. Areas.SelectMany(Declared)];

    private static Type Area([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields)] Type area) => area;

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Every type in Areas passed through Area, whose parameter keeps the public fields.")]
    private static IEnumerable<FaultPoint> Declared(Type area) => area
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Where(f => f.FieldType == typeof(FaultPoint))
        .Select(f => (FaultPoint)f.GetValue(null)!);

    public static FaultPoint? Find(string name) =>
        All.Cast<FaultPoint?>().FirstOrDefault(p => p!.Value.Name == name);
}
