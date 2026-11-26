using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Harness.Time;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using StackExchange.Redis;

namespace Reeve.Cluster.Harness.Replicas;

internal sealed class Replica : IAsyncDisposable
{
    private readonly object gate = new();
    private readonly SemaphoreSlim transitions = new(1, 1);
    private readonly Action<string> log;
    private IHost? host;
    private LinkHold? pauseHold;
    private readonly HarnessRun run;
    private IDisposable? phaseSubscription;
    private State state = State.Created;
    private bool paused;

    private Replica(
        string id,
        ClusterOptions options,
        DriftingTimeProvider clock,
        HarnessFaultPoints faults,
        ReconcilerScript reconcilers,
        ReplicaLink? link,
        IHost host,
        HarnessRun run,
        Action<string> log)
    {
        this.run = run;
        Id = id;
        Options = options;
        Clock = clock;
        Faults = faults;
        Reconcilers = reconcilers;
        Link = link;
        this.host = host;
        this.log = log;
    }

    private enum State
    {
        Created,
        Running,
        Stopped,
        Crashed,
    }

    public string Id { get; }

    public ClusterOptions Options { get; }

    public DriftingTimeProvider Clock { get; }

    public HarnessFaultPoints Faults { get; }

    public ReconcilerScript Reconcilers { get; }

    public ReplicaLink? Link { get; }

    public bool HostGone { get; private set; }

    public IServiceProvider Services => (host ?? throw new ObjectDisposedException(Id)).Services;

    public Uri Url
    {
        get
        {
            var addresses = Services.GetService<Microsoft.AspNetCore.Hosting.Server.IServer>()?.Features.Get<IServerAddressesFeature>()?.Addresses
                ?? throw new InvalidOperationException($"Replica '{Id}' has no web server: its spec sets no ConfigureWebApp.");
            return new Uri(addresses.FirstOrDefault() ?? throw new InvalidOperationException($"Replica '{Id}' web server is not listening yet."));
        }
    }

    public bool IsRunning
    {
        get
        {
            lock (gate)
            {
                return state == State.Running;
            }
        }
    }

    public bool IsCrashed
    {
        get
        {
            lock (gate)
            {
                return state == State.Crashed;
            }
        }
    }

    private static readonly TimeSpan StopLimit = TimeSpan.FromSeconds(30);

    public bool IsPaused
    {
        get
        {
            lock (gate)
            {
                return paused;
            }
        }
    }

    internal static Replica Build(
        string id,
        ReplicaSpec spec,
        ReplicaLink? link,
        HarnessRun run,
        Action<string> log)
    {
        var connectionString = link?.PostgresConnectionString
            ?? spec.ConnectionString
            ?? throw new InvalidOperationException($"Replica '{id}' has no connection string: run it on a stack, or give its spec one.");
        connectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            CommandTimeout = (int)spec.DriverTimeout.TotalSeconds,
            Timeout = (int)spec.DriverTimeout.TotalSeconds,
        }.ConnectionString;

        var clock = new DriftingTimeProvider();
        if (spec.ClockRate != 1.0)
        {
            clock.SetRate(spec.ClockRate);
        }

        var faults = new HarnessFaultPoints(id, clock, run.Arrivals);
        var script = new ReconcilerScript(clock);

        void Apply(ClusterOptions o)
        {
            (spec.Profile ?? TestProfile.ApplyStallTolerant)(o);
            o.ReplicaId = id;
            o.ConnectionString = connectionString;
            spec.Configure?.Invoke(o);
        }

        var options = new ClusterOptions();
        Apply(options);

        var web = spec.ConfigureWebApp is null ? null : WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        web?.WebHost.UseKestrel(kestrel => kestrel.Listen(IPAddress.Loopback, 0));
        IHostApplicationBuilder builder = web ?? (IHostApplicationBuilder)Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Logging.AddProvider(new HarnessLoggerProvider(id, log));
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));
        builder.Services.AddSingleton<IHostLifetime, HarnessHostLifetime>();

        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddSingleton<IFaultPoints>(faults);
        builder.Services.AddSingleton<IModelEventSink>(new ReplicaModelEvents(id, clock, run.Events, () => !faults.IsAbandoned));
        builder.Services.AddSingleton(script);
        builder.Services.AddReeveCluster(new ClusterPorts(spec.Reconcilers, spec.FenceSink), Apply);
        if (spec.Redis)
        {
            var redisEndpoint = link?.RedisConnectionString
                ?? throw new InvalidOperationException($"Replica '{id}' asks for Redis, which a run without a stack does not have.");
            var redisOptions = ConfigurationOptions.Parse(redisEndpoint);
            redisOptions.AbortOnConnectFail = false;
            redisOptions.ConnectTimeout = redisOptions.SyncTimeout = redisOptions.AsyncTimeout = (int)spec.DriverTimeout.TotalMilliseconds;

            redisOptions.ReconnectRetryPolicy = new LinearRetry(1000);
            builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisOptions));
            builder.Services.AddReeveClusterRedis(o => o.KeyPrefix = run.RedisKeyPrefix);
        }

        spec.ConfigureServices?.Invoke(builder.Services);

        IHost built;
        if (web is not null)
        {
            var app = web.Build();
            spec.ConfigureWebApp!(app);
            built = app;
        }
        else
        {
            built = ((HostApplicationBuilder)builder).Build();
        }

        return new Replica(id, options, clock, faults, script, link, built, run, log);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (gate)
        {
            if (state != State.Created)
            {
                throw new InvalidOperationException($"Replica '{Id}' is {state}.");
            }

            state = State.Running;
        }

        log($"replica {Id}: start");

        if (host!.Services.GetService<IPhaseGate>() is { } phaseGate)
        {
            phaseSubscription = run.Phases.Attach(Id, phaseGate, Clock, () => !Faults.IsAbandoned);
        }

        var started = host.StartAsync(cancellationToken);
        _ = started.ContinueWith(
            t => log($"replica {Id}: the host's start ended {(t.IsCanceled ? "cancelled" : $"with {t.Exception!.InnerException?.GetType().Name}")}"),
            CancellationToken.None,
            TaskContinuationOptions.NotOnRanToCompletion,
            TaskScheduler.Default);
        Started = started;
        await Task.Yield();
    }

    public Task? Started { get; private set; }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (gate)
        {
            if (state != State.Running)
            {
                return;
            }

            state = State.Stopped;
        }

        log($"replica {Id}: stop");
        await ResumeAsync().ConfigureAwait(false);
        await host!.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task PauseAsync(CancellationToken cancellationToken = default)
    {
        await transitions.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await PauseCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            transitions.Release();
        }
    }

    private async Task PauseCoreAsync(CancellationToken cancellationToken)
    {
        lock (gate)
        {
            if (state is State.Crashed or State.Stopped)
            {
                throw new InvalidOperationException($"Replica '{Id}' is {state} and cannot be paused.");
            }

            if (paused)
            {
                return;
            }

            paused = true;
        }

        log($"replica {Id}: pause");
        Faults.Pause();
        Clock.Pause();
        if (Link is null)
        {
            RecordEnvironment(EnvironmentActions.Pause);
            return;
        }

        try
        {
            var hold = await Link.HoldAsync(cancellationToken).ConfigureAwait(false);
            lock (gate)
            {
                pauseHold = hold;
            }

            RecordEnvironment(EnvironmentActions.Pause);
        }
        catch
        {
            lock (gate)
            {
                paused = false;
            }

            Clock.Resume();
            Faults.Resume();
            throw;
        }
    }

    public async Task ResumeAsync()
    {
        await transitions.WaitAsync().ConfigureAwait(false);
        try
        {
            LinkHold? hold;
            lock (gate)
            {
                if (!paused)
                {
                    return;
                }

                paused = false;
                hold = pauseHold;
                pauseHold = null;
                if (state == State.Crashed)
                {
                    return;
                }
            }

            log($"replica {Id}: resume");

            RecordEnvironment(EnvironmentActions.Resume);
            try
            {
                if (hold is not null)
                {
                    await hold.ReleaseAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            finally
            {
                Clock.Resume();
                Faults.Resume();
            }
        }
        finally
        {
            transitions.Release();
        }
    }

    public async Task PauseForAsync(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await PauseAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await Task.Delay(duration, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await ResumeAsync().ConfigureAwait(false);
        }
    }

    public async Task CrashAsync()
    {
        lock (gate)
        {
            if (state is State.Crashed)
            {
                return;
            }

            state = State.Crashed;
        }

        log($"replica {Id}: crash");
        Faults.Abandon();
        Clock.Pause();
        RecordEnvironment(EnvironmentActions.Crash);
        if (Link is not null)
        {
            await Link.CutAsync().ConfigureAwait(false);
        }

        await DisposeHostAsync(StopLimit).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        bool graceful;
        lock (gate)
        {
            graceful = state == State.Running;
        }

        Exception? stopFailure = null;
        if (graceful)
        {
            try
            {
                using var cts = new CancellationTokenSource(StopLimit);
                await StopAsync(cts.Token).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                stopFailure = exception is OperationCanceledException
                    ? new TimeoutException($"Replica '{Id}' did not stop within {StopLimit.TotalSeconds:0} s.", exception)
                    : exception;
            }
        }

        await DisposeHostAsync(StopLimit).ConfigureAwait(false);

        if (stopFailure is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(stopFailure).Throw();
        }
    }

    internal void RecordEnvironment(string action) => run.RecordEnvironment(Id, action);

    private async Task DisposeHostAsync(TimeSpan limit)
    {
        IHost? current;
        lock (gate)
        {
            current = host;
            host = null;
        }

        if (current is null)
        {
            return;
        }

        phaseSubscription?.Dispose();

        var disposing = Task.Run(() =>
        {
            try
            {
                current.Dispose();
            }
            catch (Exception exception)
            {
                log($"replica {Id}: dispose threw {exception.GetType().Name}: {exception.Message}");
            }
        });
        if (await Task.WhenAny(disposing, Task.Delay(limit)).ConfigureAwait(false) == disposing)
        {
            HostGone = true;
        }
        else
        {
            log($"replica {Id}: the host did not finish disposing in {limit}; its proxies' ports are kept out of use");
        }
    }
}
