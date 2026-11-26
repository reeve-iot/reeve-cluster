using Reeve.Cluster.Harness.Linearizability;
using Reeve.Cluster.Harness.Processes;
using Reeve.Cluster.Harness.Stack;
using Xunit.Abstractions;

namespace Harness.Linearizability;

[Collection("linearizability-faults")]
public sealed class LinearizabilityUnderFaultsTests(ITestOutputHelper output)
{
    private const int WriterCount = 3;
    private const int ReaderCount = 3;
    private const int HubReaderCount = 2;
    private const int MinimumWrites = 20;
    private const int MinimumReads = 500;
    private const int MinimumHubReads = 20;
    private static readonly string[] Replicas = ["r1", "r2", "r3"];

    private static int HubReadsOf(HistoryRecorder recorder) => Enumerable.Range(WriterCount + ReaderCount + 1, HubReaderCount).Sum(recorder.ReadsOf);

    [Fact]
    public Task G2_holds_through_a_kill_a_drain_a_frozen_process_a_cut_link_and_a_failover() => RunAsync(faults: 4);

    [NightlyFact]
    [Trait("Category", "Nightly")]
    public async Task G2_holds_over_many_runs_with_many_faults()
    {
        var runs = int.TryParse(Environment.GetEnvironmentVariable("REEVE_LINEARIZABILITY_RUNS"), out var given) ? given : 6;
        for (var run = 1; run <= runs; run++)
        {
            output.WriteLine($"=== run {run} of {runs}");
            await RunAsync(faults: 10);
        }
    }

    private async Task RunAsync(int faults)
    {
        var stack = new ClusterStack(output.WriteLine, new ClusterStackOptions { Standby = true });
        try
        {
            await stack.InitializeAsync();
            await using var cluster = await ProcessCluster.CreateAsync(stack, output.WriteLine, Replicas);
            await stack.Pair.SetModeAsync(StandbyMode.SyncFirst);
            foreach (var id in Replicas)
            {
                await cluster.StartProcessAsync(id);
            }

            var path = Path.Combine(LinearizabilityChecker.HistoryDirectory, $"faults-{DateTime.UtcNow:yyyyMMddTHHmmss}-seed{cluster.Run.Seed.Value}.jsonl");
            var registers = new ConfigRegisters();
            var schedule = new FaultSchedule(cluster, stack, Replicas, cluster.Run.Seed, output.WriteLine);
            HistoryCounts counts;
            int hubReads;
            HistoryCounts afterFailover = null!;
            try
            {
                using (var recorder = new HistoryRecorder(path, registers.Names))
                {
                    var workload = new ConfigWorkload(
                        new ConfigClient(cluster.Client, recorder, registers),
                        new Uri(cluster.Balancer.Url, "hubs/flags"),
                        recorder,
                        registers,
                        Replicas,
                        cluster.Run.Seed,
                        TimeSpan.FromSeconds(30),
                        readPause: TimeSpan.FromMilliseconds(20));
                    await workload.SeedAsync();
                    using var stop = new CancellationTokenSource();
                    var clients = workload.RunAsync(WriterCount, ReaderCount, HubReaderCount, stop.Token);
                    try
                    {
                        schedule.FailedOver += () => afterFailover = recorder.Counts;
                        await schedule.RunAsync(faults, CancellationToken.None);

                        await WorkloadWait.UntilAsync(
                            () => recorder.Counts is var c && afterFailover is not null && c.Writes >= MinimumWrites && c.Reads >= MinimumReads && c.Writes - afterFailover.Writes >= 3
                                && HubReadsOf(recorder) >= MinimumHubReads,
                            TimeSpan.FromSeconds(120));
                    }
                    finally
                    {
                        await stop.CancelAsync();
                        await clients;
                    }

                    counts = recorder.Counts;
                    hubReads = HubReadsOf(recorder);
                }

                var verdict = await LinearizabilityChecker.CheckAsync(path, TimeSpan.FromMinutes(10));
                output.WriteLine($"seed {cluster.Run.Seed.Value}; faults {string.Join(", ", schedule.Events.Select(e => $"{e.Fault}:{e.Replica}"))}; unexpected exits {schedule.UnexpectedExits}; {(schedule.Degraded.Count == 0 ? "no degraded fault" : string.Join("; ", schedule.Degraded))}; {counts}; {verdict}");
                Assert.True(verdict.Linearizable, verdict.ToString());

                Assert.True(counts.Writes >= MinimumWrites && counts.Reads >= MinimumReads, $"Too little was recorded to say anything: {counts}.");
                Assert.True(counts.UnknownWrites + counts.FailedWrites + counts.FailedReads > 0, $"No request met a fault: {counts}.");
                Assert.True(counts.Writes - afterFailover.Writes >= 3, $"Fewer than 3 writes were acknowledged after the failover: {afterFailover} then {counts}.");
                Assert.True(hubReads >= MinimumHubReads, $"The hub readers completed {hubReads} reads.");
            }
            catch (Exception exception)
            {
                output.WriteLine($"the run failed: {exception}");
                output.WriteLine(await cluster.ReportAsync());
                throw;
            }
        }
        finally
        {
            await stack.DisposeAsync();
        }
    }
}
