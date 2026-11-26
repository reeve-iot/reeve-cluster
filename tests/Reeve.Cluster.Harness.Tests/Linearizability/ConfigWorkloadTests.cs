using System.Text.Json;
using Harness.SelfTests;
using Reeve.Cluster.Harness.Linearizability;
using Reeve.Cluster.Harness.Processes;
using Reeve.Cluster.Harness.Stack;
using Xunit.Abstractions;

namespace Harness.Linearizability;

[Collection(StackCollection.Name)]
public sealed class ConfigWorkloadTests(StackFixture stack, ITestOutputHelper output)
{
    private const int Writers = 3;
    private const int Readers = 3;
    private const int HubReaders = 2;

    [Fact]
    public async Task A_pair_with_no_fault_is_linearizable_over_HTTP_and_hub_invocations()
    {
        await using var cluster = await ProcessCluster.StartAsync(stack.Stack, output.WriteLine, "r1", "r2");
        var path = Path.Combine(LinearizabilityChecker.HistoryDirectory, $"smoke-{DateTime.UtcNow:yyyyMMddTHHmmss}.jsonl");
        var registers = new ConfigRegisters();
        HistoryCounts counts;
        try
        {
            using (var recorder = new HistoryRecorder(path, registers.Names))
            {
                var workload = new ConfigWorkload(
                    new ConfigClient(cluster.Client, recorder, registers),
                    new Uri(cluster.Balancer.Url, "hubs/flags"),
                    recorder,
                    registers,
                    ["r1", "r2"],
                    cluster.Run.Seed,
                    TimeSpan.FromSeconds(30));
                await workload.SeedAsync();
                using var stop = new CancellationTokenSource();
                var clients = workload.RunAsync(Writers, Readers, HubReaders, stop.Token);
                await Task.Delay(TimeSpan.FromSeconds(10));
                await WorkloadWait.UntilAsync(
                    () => recorder.Counts is { Writes: > 10, Reads: > 100 } && Enumerable.Range(Writers + Readers + 1, HubReaders).Sum(recorder.ReadsOf) > 20,
                    TimeSpan.FromSeconds(120));
                await stop.CancelAsync();
                await clients;
                counts = recorder.Counts;
            }

            var verdict = await LinearizabilityChecker.CheckAsync(path, TimeSpan.FromMinutes(5));
            output.WriteLine($"{counts}; {verdict}");
            Assert.True(verdict.Linearizable, verdict.ToString());
            Assert.True(counts.Writes > 10 && counts.Reads > 100, counts.ToString());
            Assert.True(HubReads(path) > 20, $"The hub clients recorded {HubReads(path)} reads.");
        }
        catch
        {
            output.WriteLine(await cluster.ReportAsync());
            throw;
        }
    }

    private static int HubReads(string path) =>
        File.ReadLines(path).Skip(1).Count(line =>
        {
            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;
            return root.GetProperty("client").GetInt32() > Writers + Readers && root.GetProperty("status").GetString() == "ok";
        });
}
