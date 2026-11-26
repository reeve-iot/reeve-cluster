using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Timing;

namespace Reeve.Cluster.Harness.Processes;

internal sealed class ProcessCluster : IAsyncDisposable
{
    private const string BalancerPort = "balancer";
    private static readonly TimeSpan ReadyLimit = TimeSpan.FromSeconds(60);

    private readonly ClusterStack stack;
    private readonly IReadOnlyDictionary<string, int> ports;
    private readonly List<SampleProcess> processes = [];
    private readonly Dictionary<string, int> generations = new(StringComparer.Ordinal);

    private ProcessCluster(ClusterStack stack, HarnessRun run, IReadOnlyDictionary<string, int> ports, Balancer balancer, Action<string> log, Action<ClusterOptions>? profile)
    {
        Profile = profile;
        this.stack = stack;
        Run = run;
        this.ports = ports;
        Balancer = balancer;
        Log = log;
        Client = new HttpClient { BaseAddress = balancer.Url, Timeout = TimeSpan.FromSeconds(90) };
    }

    public HarnessRun Run { get; }

    public Action<ClusterOptions>? Profile { get; }

    public Balancer Balancer { get; }

    public Action<string> Log { get; }

    public HttpClient Client { get; }

    public static ClusterOptions Options(string replicaId, Action<ClusterOptions>? configure = null, Action<ClusterOptions>? profile = null)
    {
        var options = TestProfile.Options(replicaId, "unused", profile);
        options.ReadIndexTimeout = TimeSpan.FromSeconds(8);
        options.ActionTimeBudget = TimeSpan.FromSeconds(5);
        options.ProxyReadTimeout = TimeSpan.FromSeconds(60);
        options.CheckSynchronousReplication = false;
        configure?.Invoke(options);
        return options;
    }

    public static TimeSpan ProxyReadTimeout { get; } = Options("r").ProxyReadTimeout!.Value;

    public static async Task<ProcessCluster> CreateAsync(ClusterStack stack, Action<string> log, string[] ids, TimeSpan? balancerReadTimeout = null, Action<ClusterOptions>? profile = null, bool migrate = true)
    {
        var run = await HarnessRun.BeginAsync(log, stack).ConfigureAwait(false);
        try
        {
            if (migrate)
            {
                await run.MigrateAsync().ConfigureAwait(false);
            }

            var ports = LoopbackPorts.Reserve([.. ids, BalancerPort]);
            var balancer = await Balancer.StartAsync([.. ids.Select(id => (id, ports[id]))], ports[BalancerPort], balancerReadTimeout ?? ProxyReadTimeout).ConfigureAwait(false);
            return new ProcessCluster(stack, run, ports, balancer, log, profile);
        }
        catch
        {
            await run.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public static async Task<ProcessCluster> StartAsync(ClusterStack stack, Action<string> log, params string[] ids)
    {
        var cluster = await CreateAsync(stack, log, ids).ConfigureAwait(false);
        try
        {
            foreach (var id in ids)
            {
                await cluster.StartProcessAsync(id).ConfigureAwait(false);
            }

            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public SampleProcess this[string id] => processes.Last(p => p.Id == id);

    public async Task<SampleProcess> StartProcessAsync(string id, Action<ClusterOptions>? configure = null, SampleBuild? build = null)
    {
        var started = await StartWithoutWaitingAsync(id, configure, build).ConfigureAwait(false);
        await started.WaitReadyAsync(ReadyLimit).ConfigureAwait(false);
        return started;
    }

    public async Task<SampleProcess> StartWithoutWaitingAsync(string id, Action<ClusterOptions>? configure = null, SampleBuild? build = null, IEnumerable<string>? arguments = null)
    {
        var generation = generations[id] = generations.GetValueOrDefault(id) + 1;
        var link = await stack.CreateLinkAsync($"{Run.RunId}-{id}#{generation}", Run.Database).ConfigureAwait(false);
        var options = Options(id, configure, Profile);
        SampleProcess started;
        try
        {
            started = SampleProcess.Start(id, ports[id], link, options, Log, build?.AssemblyPath, build is null ? null : build.Environment(Run, link), arguments);
        }
        catch
        {
            await link.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        processes.Add(started);
        return started;
    }

    public Task<HttpResponseMessage> ThroughAsync(string requestId, HttpMethod method, string path, string? first = null, HttpContent? content = null)
    {
        var request = new HttpRequestMessage(method, path) { Content = content };
        request.Headers.Add(Balancer.IdHeader, requestId);
        if (first is not null)
        {
            request.Headers.Add(Balancer.FirstHeader, first);
        }

        return Client.SendAsync(request);
    }

    public async Task<string> ReportAsync() =>
        string.Join(Environment.NewLine, new[] { Run.Report() }
            .Concat(processes.Select(p => $"--- {p.Id} (exited: {p.HasExited}){Environment.NewLine}{string.Join(Environment.NewLine, p.Output.TakeLast(60))}"))
            .Append(await Balancer.LogAsync().ConfigureAwait(false)));

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        List<Exception>? failures = null;
        foreach (var process in processes.AsEnumerable().Reverse())
        {
            await Guard(async () => await process.DisposeAsync().ConfigureAwait(false)).ConfigureAwait(false);
            await Guard(async () =>
            {
                try
                {
                    await process.Link.DisposeAsync().ConfigureAwait(false);
                }
                catch (AggregateException exception)
                {
                    Log($"cleanup: the link of {process.Id} was not deleted: {exception.InnerExceptions.Count} refusal(s): {string.Join("; ", exception.InnerExceptions.Select(e => e.Message))}");
                }
            }).ConfigureAwait(false);
        }

        await Guard(async () => await Balancer.DisposeAsync().ConfigureAwait(false)).ConfigureAwait(false);
        await Guard(async () => await Run.DisposeAsync().ConfigureAwait(false)).ConfigureAwait(false);
        if (failures is not null)
        {
            throw new AggregateException("Disposing the process cluster failed.", failures);
        }

        async Task Guard(Func<Task> step)
        {
            try
            {
                await step().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }
    }
}
