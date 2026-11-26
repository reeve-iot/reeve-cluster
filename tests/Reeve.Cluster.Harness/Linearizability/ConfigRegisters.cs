using System.Text.Json;

namespace Reeve.Cluster.Harness.Linearizability;

internal sealed class ConfigRegisters
{
    private readonly Dictionary<string, long> counters;

    public ConfigRegisters(int quotas = 3, int settings = 3)
    {
        Quotas = [.. Enumerable.Range(0, quotas).Select(i => $"q{i}")];
        Settings = [.. Enumerable.Range(0, settings).Select(i => $"s{i}")];
        Names = [.. Quotas, .. Settings];
        counters = Names.ToDictionary(name => name, _ => 0L);
    }

    public IReadOnlyList<string> Quotas { get; }

    public IReadOnlyList<string> Settings { get; }

    public IReadOnlyList<string> Names { get; }

    public int NextValue(string name)
    {
        lock (counters)
        {
            return checked((int)++counters[name]);
        }
    }

    public object Body(IReadOnlyDictionary<string, int> values) => new
    {
        quotas = values.Where(v => Quotas.Contains(v.Key)).ToDictionary(v => v.Key, v => v.Value),
        settings = values.Where(v => Settings.Contains(v.Key)).ToDictionary(v => v.Key, v => v.Value),
    };

    public Dictionary<string, int> Parse(JsonElement config)
    {
        var values = Names.ToDictionary(name => name, _ => 0);
        Fill(config, "quotas", Quotas);
        Fill(config, "settings", Settings);
        return values;

        void Fill(JsonElement root, string property, IReadOnlyList<string> names)
        {
            if (!root.TryGetProperty(property, out var map))
            {
                throw new JsonException($"The answer has no '{property}'.");
            }

            foreach (var name in names)
            {
                if (map.TryGetProperty(name, out var value))
                {
                    values[name] = value.GetInt32();
                }
            }
        }
    }
}
