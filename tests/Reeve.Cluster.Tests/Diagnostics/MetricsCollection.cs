namespace Unit.Diagnostics;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MetricsCollection
{
    public const string Name = "Metrics";
}
