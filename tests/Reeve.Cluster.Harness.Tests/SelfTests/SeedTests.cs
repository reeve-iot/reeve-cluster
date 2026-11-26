using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Seeds;

namespace Harness.SelfTests;

public sealed class SeedTests
{
    [Fact]
    public void A_seed_is_written_to_the_run_log_before_anything_uses_it()
    {
        var lines = new List<string>();

        _ = new RunSeed(1234, lines.Add);

        var line = Assert.Single(lines);
        Assert.Contains("1234", line, StringComparison.Ordinal);
        Assert.Contains(RunSeed.EnvironmentVariable, line, StringComparison.Ordinal);
    }

    [Fact]
    public void The_same_seed_draws_the_same_decisions_and_another_seed_draws_others()
    {
        static IReadOnlyList<string> Draw(long seed)
        {
            var random = new RunSeed(seed).Fork("schedule");
            random.NextInt("pause", 0, 1000);
            random.NextDouble("drift");
            random.Pick("outage", DatabaseOutages());
            random.PickIndex("release", 5);
            return random.Decisions;
        }

        Assert.Equal(Draw(42), Draw(42));
        Assert.NotEqual(Draw(42), Draw(43));
    }

    [Fact]
    public void Forks_of_one_seed_are_independent_of_each_other()
    {
        var seed = new RunSeed(7);
        var a = seed.Fork("a");
        var b = seed.Fork("b");
        var first = a.NextInt("x", 0, int.MaxValue);

        var a2 = new RunSeed(7).Fork("a");
        _ = b.NextInt("noise", 0, int.MaxValue);
        Assert.Equal(first, a2.NextInt("x", 0, int.MaxValue));
        Assert.NotEqual(first, b.NextInt("x", 0, int.MaxValue));
    }

    [Fact]
    public async Task A_seed_replays_the_same_release_schedule_of_held_arrivals()
    {
        async Task<IReadOnlyList<long>> ReleaseOrder(long seed)
        {
            var faults = new HarnessFaultPoints("r1", TimeProvider.System, new ArrivalLog(new RunSequence()));
            var random = new RunSeed(seed).Fork("release");
            using var hold = faults.Hold(LogFaultPoints.PassBegin);

            var reached = new List<Task>();
            for (var i = 0; i < 8; i++)
            {
                reached.Add(faults.ReachAsync(LogFaultPoints.PassBegin).AsTask());
                await hold.WaitReachedAsync(i + 1);
            }

            var order = new List<long>();
            while (hold.HeldNow > 0)
            {
                var held = hold.Held;
                var index = random.PickIndex("next", held.Count);
                order.Add(held[index].Sequence);
                Assert.True(hold.ReleaseAt(index));
            }

            await Task.WhenAll(reached).WaitAsync(TimeSpan.FromSeconds(10));
            return order;
        }

        var first = await ReleaseOrder(99);

        Assert.Equal(first, await ReleaseOrder(99));
        Assert.NotEqual(first, await ReleaseOrder(100));
        Assert.NotEqual(first.Order(), first);
    }

    private static IReadOnlyList<TimeSpan> DatabaseOutages() => Reeve.Cluster.Harness.Stack.DatabaseFaults.Outages;

    [Fact]
    public void The_outage_lengths_offered_are_the_four_the_spec_names()
    {
        Assert.Equal([5, 10, 20, 30], DatabaseOutages().Select(d => (int)d.TotalSeconds));
    }
}
