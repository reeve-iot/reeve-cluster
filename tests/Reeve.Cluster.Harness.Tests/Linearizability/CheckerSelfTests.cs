using System.Text.Json;
using Reeve.Cluster.Harness.Linearizability;

namespace Harness.Linearizability;

public sealed class CheckerSelfTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromMinutes(1);

    private static string Testdata => Path.Combine(LinearizabilityChecker.RepositoryRoot(), "tests", "linearizability-checker", "testdata");

    public static TheoryData<string> FixtureNames() => Names(Testdata);

    public static TheoryData<string> BadInputNames() => Names(Path.Combine(Testdata, "bad-input"));

    [Theory]
    [MemberData(nameof(FixtureNames))]
    public async Task A_fixture_gets_the_verdict_its_name_gives(string name)
    {
        using var copy = new Copy(Path.Combine(Testdata, name));

        var verdict = await LinearizabilityChecker.CheckAsync(copy.Path, Limit);

        Assert.True(name.StartsWith("illegal-", StringComparison.Ordinal) ? verdict.Violation : verdict.Linearizable, $"{name}: {verdict}");
    }

    [Fact]
    public async Task A_torn_history_is_rejected_and_pictured()
    {
        using var copy = new Copy(Path.Combine(Testdata, "illegal-torn.jsonl"));

        var verdict = await LinearizabilityChecker.CheckAsync(copy.Path, Limit);

        Assert.True(verdict.Violation, verdict.ToString());
        Assert.NotNull(verdict.Visualization);
        Assert.True(new FileInfo(verdict.Visualization).Length > 0);
    }

    [Theory]
    [MemberData(nameof(BadInputNames))]
    public async Task A_history_that_breaks_the_format_is_refused_with_no_verdict(string name)
    {
        using var copy = new Copy(Path.Combine(Testdata, "bad-input", name));

        var verdict = await LinearizabilityChecker.CheckAsync(copy.Path, Limit);

        Assert.Equal(3, verdict.ExitCode);
    }

    [Fact]
    public async Task A_file_that_is_not_json_is_refused_with_no_verdict()
    {
        using var scratch = new Copy(null);
        await File.WriteAllTextAsync(scratch.Path, "not json\n");

        var verdict = await LinearizabilityChecker.CheckAsync(scratch.Path, Limit);

        Assert.Equal(3, verdict.ExitCode);
    }

    [Fact]
    public async Task A_history_the_checker_cannot_finish_in_time_has_no_verdict()
    {
        using var scratch = new Copy(null);
        WriteHeavyHistory(scratch.Path, clients: 24, operationsPerClient: 400);

        var verdict = await LinearizabilityChecker.CheckAsync(scratch.Path, TimeSpan.FromSeconds(1));

        Assert.True(verdict.TimedOut, verdict.ToString());
        Assert.False(verdict.Linearizable);
        Assert.Contains("\"result\":\"Unknown\"", verdict.Summary, StringComparison.Ordinal);
    }

    private static TheoryData<string> Names(string directory)
    {
        var names = new TheoryData<string>();
        foreach (var file in Directory.GetFiles(directory, "*.jsonl").Order())
        {
            names.Add(Path.GetFileName(file));
        }

        return names;
    }

    private static void WriteHeavyHistory(string path, int clients, int operationsPerClient)
    {
        var random = new Random(1);
        string[] names = ["T0", "T1", "T2", "T3"];
        var counters = names.ToDictionary(n => n, _ => 0);
        var planned = new List<(double Point, int Client, long Call, long Ret, Dictionary<string, int>? Writes)>();
        for (var client = 0; client < clients; client++)
        {
            var t = (long)random.Next(0, 100);
            for (var i = 0; i < operationsPerClient; i++)
            {
                var call = t + random.Next(1, 10);
                var ret = call + random.Next(50, 500);
                Dictionary<string, int>? writes = null;
                if (random.NextDouble() < 0.2)
                {
                    writes = [];
                    foreach (var name in names.OrderBy(_ => random.Next()).Take(random.Next(1, 4)))
                    {
                        writes[name] = ++counters[name];
                    }
                }

                planned.Add((call + (random.NextDouble() * (ret - call)), client, call, ret, writes));
                t = ret;
            }
        }

        var state = names.ToDictionary(n => n, _ => 0);
        using var file = new StreamWriter(path);
        file.WriteLine(JsonSerializer.Serialize(new { format = "reeve-history/1", types = names }));
        var id = 0;
        foreach (var (_, client, call, ret, writes) in planned.OrderBy(p => p.Point))
        {
            if (writes is not null)
            {
                foreach (var (name, value) in writes)
                {
                    state[name] = value;
                }
            }

            file.WriteLine(JsonSerializer.Serialize(new { id = ++id, client, replica = 0, kind = writes is null ? "read" : "write", call, ret, status = "ok", writes, reads = writes is null ? state : null }));
        }
    }

    private sealed class Copy : IDisposable
    {
        private readonly DirectoryInfo directory = Directory.CreateTempSubdirectory("linearizability-selftest-");

        public Copy(string? fixture)
        {
            Path = System.IO.Path.Combine(directory.FullName, fixture is null ? "history.jsonl" : System.IO.Path.GetFileName(fixture));
            if (fixture is not null)
            {
                File.Copy(fixture, Path);
            }
        }

        public string Path { get; }

        public void Dispose() => directory.Delete(recursive: true);
    }
}
