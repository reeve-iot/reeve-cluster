namespace Reeve.Cluster.Harness.Faults;

internal sealed class UnknownFaultPointException(string name)
    : InvalidOperationException($"'{name}' is not an entry of the fault-point catalogue.")
{
    public string Name { get; } = name;

    public static FaultPoint Resolve(string name) =>
        FaultPointCatalogue.Find(name) ?? throw new UnknownFaultPointException(name);
}
