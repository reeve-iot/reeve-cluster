using System.Collections.Concurrent;
using Reeve.Cluster;
using Reeve.Cluster.Log;

namespace Unit.Generations;

public sealed class GenerationStressTests
{
    private static readonly string[] Types = ["t0", "t1", "t2", "t3"];

    [Fact]
    public async Task I6_readers_never_see_a_mix_of_two_generations()
    {
        var store = Stores.Create(Types);
        var expected = new ConcurrentDictionary<long, long[]>();
        var states = Types.Select(_ => 0L).ToArray();
        expected[0] = [.. states];
        await store.PublishAsync(0, null, [.. Types.Select(t => (t, (object)0L))]);

        using var stop = new CancellationTokenSource();
        var violations = new ConcurrentQueue<string>();
        var reads = 0L;
        var readers = Enumerable.Range(0, 4).Select(seed => Task.Run(() =>
        {
            var random = new Random(seed);
            while (!stop.IsCancellationRequested)
            {
                var generation = store.Current!;
                var row = expected[generation.Head];
                foreach (var i in Enumerable.Range(0, Types.Length).OrderBy(_ => random.Next()))
                {
                    if (random.Next(4) == 0)
                    {
                        Thread.Yield();
                    }

                    var seen = (long)generation.Get(Types[i]);
                    if (seen != row[i])
                    {
                        violations.Enqueue($"head {generation.Head}: {Types[i]} = {seen}, expected {row[i]}");
                    }
                }

                Interlocked.Increment(ref reads);
            }
        })).ToArray();

        var publishRandom = new Random(42);
        for (long head = 1; head <= 20_000; head++)
        {
            var dirty = Enumerable.Range(0, Types.Length).Where(_ => publishRandom.Next(2) == 0).ToArray();
            foreach (var i in dirty)
            {
                states[i] = head;
            }

            expected[head] = [.. states];
            await store.PublishAsync(head, null, [.. dirty.Select(i => (Types[i], (object)head))]);
        }

        await stop.CancelAsync();
        await Task.WhenAll(readers);

        Assert.Empty(violations);
        Assert.True(Interlocked.Read(ref reads) > 0);
    }

    [Fact]
    public async Task I7_an_activation_runs_only_while_its_type_state_is_current_and_none_outlives_its_supersession()
    {
        var store = Stores.Create(Types);
        await store.PublishAsync(0, null, [.. Types.Select(t => (t, (object)0L))]);

        var dispatch = new ConcurrentDictionary<(string Type, object State), byte>();
        var violations = new ConcurrentQueue<string>();
        var activated = 0L;
        using var stop = new CancellationTokenSource();

        var activators = Enumerable.Range(0, 4).Select(seed => Task.Run(() =>
        {
            var random = new Random(seed);
            while (!stop.IsCancellationRequested)
            {
                var generation = store.Current!;
                var type = Types[random.Next(Types.Length)];
                var state = generation.Get(type);
                Thread.Yield();
                var ok = generation.TryActivate(type, () =>
                {
                    if (!ReferenceEquals(store.Current!.Get(type), state))
                    {
                        violations.Enqueue($"{type}: activated a replaced state");
                    }

                    dispatch[(type, state)] = 0;
                });
                if (ok)
                {
                    generation.Superseded(type).Register(() => dispatch.TryRemove((type, state), out _));
                    Interlocked.Increment(ref activated);
                }
            }
        })).ToArray();

        var publishRandom = new Random(7);
        for (long head = 1; head <= 20_000; head++)
        {
            var dirty = Enumerable.Range(0, Types.Length).Where(_ => publishRandom.Next(2) == 0);
            await store.PublishAsync(head, null, [.. dirty.Select(i => (Types[i], (object)head))]);
        }

        await stop.CancelAsync();
        await Task.WhenAll(activators);

        Assert.Empty(violations);

        var current = store.Current!;
        SpinWait.SpinUntil(() => dispatch.Keys.All(entry => ReferenceEquals(current.Get(entry.Type), entry.State)), TimeSpan.FromSeconds(30));
        Assert.All(dispatch.Keys, entry => Assert.Same(current.Get(entry.Type), entry.State));
        Assert.True(Interlocked.Read(ref activated) > 0);
    }
}
