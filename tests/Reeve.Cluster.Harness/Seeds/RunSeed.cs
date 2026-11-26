namespace Reeve.Cluster.Harness.Seeds;

internal sealed class RunSeed
{
    public const string EnvironmentVariable = "REEVE_HARNESS_SEED";

    private readonly Action<string> log;

    public RunSeed(long value, Action<string>? log = null)
    {
        Value = value;
        this.log = log ?? (_ => { });
        this.log($"harness seed: {value} (replay with {EnvironmentVariable}={value})");
    }

    public long Value { get; }

    public static RunSeed FromEnvironmentOrRandom(Action<string>? log = null)
    {
        var text = Environment.GetEnvironmentVariable(EnvironmentVariable);
        var value = long.TryParse(text, out var parsed) ? parsed : Random.Shared.NextInt64();
        return new RunSeed(value, log);
    }

    public SeededRandom Fork(string stream)
    {
        var hash = 14695981039346656037UL;
        foreach (var c in stream)
        {
            hash = unchecked((hash ^ c) * 1099511628211UL);
        }

        return new SeededRandom(stream, unchecked((long)(hash ^ (ulong)Value)));
    }
}
