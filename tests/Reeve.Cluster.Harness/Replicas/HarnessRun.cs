using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Observation;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Seeds;
using Reeve.Cluster.Harness.Stack;

namespace Reeve.Cluster.Harness.Replicas;

internal sealed class HarnessRun : IAsyncDisposable
{
    private readonly object gate = new();
    private readonly List<Replica> replicas = [];
    private readonly Dictionary<string, int> processes = new(StringComparer.Ordinal);
    private readonly ClusterStack? stack;
    private readonly DatabaseObserver? databaseObserver;

    private HarnessRun(ClusterStack? stack, RunDatabase? database, RunSeed seed, Action<string> log)
    {
        this.stack = stack;
        Database = database;
        Seed = seed;
        Log = log;
        RunId = Guid.NewGuid().ToString("N")[..8];
        Sequence = new RunSequence();
        Arrivals = new ArrivalLog(Sequence);
        Events = new ModelEventLog(Sequence);
        Phases = new PhaseTimeline(Sequence);
        databaseObserver = database is null ? null : new DatabaseObserver(this);
    }

    public string RunId { get; }

    public RunSeed Seed { get; }

    public Action<string> Log { get; }

    public RunSequence Sequence { get; }

    public ArrivalLog Arrivals { get; }

    public ModelEventLog Events { get; }

    public PhaseTimeline Phases { get; }


    public DatabaseObserver Observer => databaseObserver ?? throw new InvalidOperationException("This run has no stack, so no database to observe.");

    public ClusterStack? Stack => stack;

    public Timing.StallProbe Stalls => Timing.StallProbe.Shared;

    public RunDatabase? Database { get; }

    public string RedisKeyPrefix => $"{RunId}:";

    public static async Task<HarnessRun> BeginAsync(Action<string> log, ClusterStack? stack = null, long? seed = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(log);
        var runSeed = seed is { } given ? new RunSeed(given, log) : RunSeed.FromEnvironmentOrRandom(log);
        var database = stack is null ? null : await stack.CreateRunDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return new HarnessRun(stack, database, runSeed, log);
    }

    public IReadOnlyList<Replica> Replicas
    {
        get
        {
            lock (gate)
            {
                return [.. replicas];
            }
        }
    }

    public Replica Get(string id)
    {
        lock (gate)
        {
            return replicas.LastOrDefault(r => r.Id == id && !r.IsCrashed)
                ?? throw new KeyNotFoundException($"No live replica '{id}' in this run.");
        }
    }

    public Task<NpgsqlConnection> OpenObserverConnectionAsync(CancellationToken cancellationToken = default) =>
        (Database ?? throw new InvalidOperationException("This run has no stack, so no database.")).OpenObserverConnectionAsync(cancellationToken);

    public void RecordEnvironment(string? replica, string action, params (string Key, object? Value)[] state)
    {
        var values = new Dictionary<string, object?>();
        if (replica is not null)
        {
            values[Stimuli.EnvironmentActions.ReplicaKey] = replica;
        }

        foreach (var (key, value) in state)
        {
            values[key] = value;
        }

        Events.Record(replica ?? "world", new ModelEvent(action, values), TimeProvider.System);
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        var database = Database ?? throw new InvalidOperationException("This run has no stack, so no database.");
        var services = new ServiceCollection();
        services.AddReeveClusterMigrations(o => o.ConnectionString = database.ObserverConnectionString);
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<IClusterMigrationRunner>().MigrateAsync(cancellationToken).ConfigureAwait(false);
    }

    public string Report()
    {
        var lines = new List<string> { $"harness seed: {Seed.Value} (replay with {RunSeed.EnvironmentVariable}={Seed.Value})" };
        var entries = new List<(long Sequence, string Text)>();
        entries.AddRange(Arrivals.Snapshot().Select(a => (a.Sequence, $"{a.Replica}: reached {a.Point}")));
        entries.AddRange(Events.Snapshot().Select(e => (e.Sequence, $"{e.Replica}: event {e.Action}")));
        entries.AddRange(Phases.Snapshot().Select(c => (c.Sequence, $"{c.Replica}: phase {c.From} -> {c.To}")));
        lines.AddRange(entries.OrderBy(e => e.Sequence).Select(e => $"{e.Sequence,5}  {e.Text}"));
        return string.Join(Environment.NewLine, lines);
    }

    public async Task<Replica> StartReplicaAsync(string id, ReplicaSpec? spec = null, CancellationToken cancellationToken = default)
    {
        var replica = await PrepareAsync(id, spec).ConfigureAwait(false);
        await replica.StartAsync(cancellationToken).ConfigureAwait(false);
        return replica;
    }

    public async Task<Replica> PrepareAsync(string id, ReplicaSpec? spec = null)
    {
        int process;
        lock (gate)
        {
            processes[id] = process = processes.GetValueOrDefault(id) + 1;
        }

        ReplicaLink? link = null;
        if (stack is not null)
        {
            link = await stack.CreateLinkAsync($"{RunId}-{id}#{process}", Database).ConfigureAwait(false);
        }

        Replica replica;
        try
        {
            replica = Replica.Build(id, spec ?? new ReplicaSpec(), link, this, Log);
        }
        catch
        {
            if (link is not null)
            {
                await link.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }

        lock (gate)
        {
            replicas.Add(replica);
        }

        if (process > 1)
        {
            replica.RecordEnvironment(Stimuli.EnvironmentActions.Restart);
        }

        return replica;
    }

    public async Task<IReadOnlyList<Replica>> StartReplicasAsync(int count, Func<string, ReplicaSpec>? spec = null, CancellationToken cancellationToken = default)
    {
        var started = new List<Replica>();
        for (var i = 1; i <= count; i++)
        {
            var id = $"r{i}";
            started.Add(await StartReplicaAsync(id, spec?.Invoke(id), cancellationToken).ConfigureAwait(false));
        }

        return started;
    }

    public async ValueTask DisposeAsync()
    {
        Replica[] all;
        lock (gate)
        {
            all = [.. replicas];
            replicas.Clear();
        }

        List<Exception>? failures = null;
        foreach (var replica in all.Reverse())
        {
            try
            {
                await replica.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }

            if (replica.Link is { } link)
            {
                try
                {
                    await link.DisposeAsync(releasePorts: replica.HostGone).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    (failures ??= []).Add(exception);
                }
            }
        }

        if (Database is not null)
        {
            try
            {
                await Database.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException("Disposing the harness run failed.", failures);
        }
    }
}
