using Reeve.Cluster.Harness.Seeds;

namespace Reeve.Cluster.Harness.Linearizability;

internal sealed class ConfigWorkload(
    ConfigClient http,
    Uri hubUrl,
    HistoryRecorder recorder,
    ConfigRegisters registers,
    IReadOnlyList<string> replicas,
    RunSeed seed,
    TimeSpan requestTimeout,
    TimeSpan readPause = default)
{
    private const double WriterWriteFraction = 0.8;
    private static readonly TimeSpan Backoff = TimeSpan.FromMilliseconds(50);

    public async Task SeedAsync()
    {
        foreach (var name in registers.Names)
        {
            var outcome = await http.WriteAsync(0, 1, replicas[0], new Dictionary<string, int> { [name] = registers.NextValue(name) }, requestTimeout).ConfigureAwait(false);
            if (outcome != Outcome.Ok)
            {
                throw new InvalidOperationException($"Seeding register {name} answered {outcome}.");
            }
        }
    }

    public Task RunAsync(int writers, int readers, int hubReaders, CancellationToken stop)
    {
        var loops = new List<Task>();
        for (var client = 1; client <= writers + readers + hubReaders; client++)
        {
            var id = client;
            loops.Add(Task.Run(
                () => id <= writers ? HttpClientLoopAsync(id, WriterWriteFraction, stop) : id <= writers + readers ? HttpClientLoopAsync(id, 0, stop) : HubClientLoopAsync(id, stop),
                CancellationToken.None));
        }

        return Task.WhenAll(loops);
    }

    private async Task HttpClientLoopAsync(int client, double writeFraction, CancellationToken stop)
    {
        var random = seed.Fork($"linearizability/client-{client}");
        while (!stop.IsCancellationRequested)
        {
            var (first, replica) = Choose(random);
            bool completed;
            if (random.NextDouble("op") < writeFraction)
            {
                var count = random.NextInt("registers", 1, 4);
                var values = new Dictionary<string, int>();
                while (values.Count < count)
                {
                    var name = random.Pick("register", registers.Names);
                    if (!values.ContainsKey(name))
                    {
                        values[name] = registers.NextValue(name);
                    }
                }

                completed = await http.WriteAsync(client, replica, first, values, requestTimeout).ConfigureAwait(false) == Outcome.Ok;
            }
            else
            {
                completed = await http.ReadAsync(client, replica, first, requestTimeout).ConfigureAwait(false);
            }

            await PauseAsync(random, completed).ConfigureAwait(false);
        }
    }

    private async Task HubClientLoopAsync(int client, CancellationToken stop)
    {
        var random = seed.Fork($"linearizability/client-{client}");
        var (first, replica) = Choose(random);
        await using var reader = new HubConfigReader(hubUrl, first, recorder, registers);
        while (!stop.IsCancellationRequested)
        {
            await PauseAsync(random, await reader.ReadAsync(client, replica, requestTimeout).ConfigureAwait(false)).ConfigureAwait(false);
        }
    }

    private Task PauseAsync(SeededRandom random, bool completed)
    {
        var pause = readPause > TimeSpan.Zero ? random.NextDuration("pause", TimeSpan.Zero, 2 * readPause) : TimeSpan.Zero;
        return !completed ? Task.Delay(Backoff, CancellationToken.None) : pause > TimeSpan.Zero ? Task.Delay(pause, CancellationToken.None) : Task.CompletedTask;
    }

    private (string? First, int Replica) Choose(SeededRandom random)
    {
        if (random.NextDouble("balancer") < 0.1)
        {
            return (null, 0);
        }

        var first = random.Pick("replica", replicas);
        return (first, replicas.ToList().IndexOf(first) + 1);
    }
}
