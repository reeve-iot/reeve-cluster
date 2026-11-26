using System.Diagnostics;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;

namespace Harness.Placer;

[Collection(StackCollection.Name)]
public sealed class RebalanceTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(10);

    private static readonly string[] Kinds = ["k0", "k1", "k2", "k3"];

    private static readonly int Placed = Kinds.Length + 4;

    [Fact]
    public async Task A_joining_member_gets_units_one_at_a_time_and_none_within_StabilisationWindow_of_its_join()
    {
        await using var rig = await PlacerRig.CreateAsync(stack, output.WriteLine);
        var spec = new ReplicaSpec
        {
            Configure = o => o.StabilisationWindow = Window,
            ConfigureServices = services =>
            {
                services.AddSingleton(rig.Log);
                services.AddSingleton(new WriterPlan(0, TimeSpan.Zero));
                foreach (var kind in Kinds)
                {
                    services.AddClusterUnit<WidgetUnit>(kind);
                }
            },
        };
        await rig.StartAsync("r1", spec);
        await PlacerRig.UntilAsync(async () => (await LoadsAsync(rig)).GetValueOrDefault("r1") == Placed, "all the placed units on r1");

        await rig.StartAsync("r2", spec);
        var joined = Stopwatch.GetTimestamp();
        var mostOpen = 0;
        await PlacerRig.UntilAsync(
            async () =>
            {
                var loads = await LoadsAsync(rig);
                mostOpen = Math.Max(mostOpen, Placed - loads.Values.Sum());
                return loads.GetValueOrDefault("r1") == 4 && loads.GetValueOrDefault("r2") == 4;
            },
            "four units on r1 and four on r2");

        await PlacerRig.UntilAsync(() => Task.FromResult(rig.Log.Starts.Count(s => s.Replica == "r2") >= 4), "r2 running its four units");
        var first = rig.Log.Starts.Where(s => s.Replica == "r2").Min(s => s.At);
        var firstAfter = Stopwatch.GetElapsedTime(joined, first);
        output.WriteLine($"r2's first unit started {firstAfter} after it served; at most {mostOpen} unit(s) unowned at a sample");

        Assert.True(firstAfter >= Window - TimeSpan.FromMilliseconds(200), $"r2's first unit started {firstAfter} after it served, within {Window}");
        Assert.True(mostOpen <= 1, $"{mostOpen} units were unowned at once: more than one move at a time");
        Assert.Equal(4, rig.Log.Starts.Count(s => s.Replica == "r2"));
    }

    private static async Task<Dictionary<string, int>> LoadsAsync(PlacerRig rig)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(
            "SELECT owner_replica, count(*) FROM reeve_cluster.owned WHERE kind <> @placer AND owner_replica IS NOT NULL GROUP BY owner_replica",
            connection);
        command.Parameters.AddWithValue("placer", Placement.PlacerKind);
        await using var reader = await command.ExecuteReaderAsync();
        var loads = new Dictionary<string, int>(StringComparer.Ordinal);
        while (await reader.ReadAsync())
        {
            loads[reader.GetString(0)] = (int)reader.GetInt64(1);
        }

        return loads;
    }
}
