using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Fence;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Wakeup;
using Unit.Membership;
using Unit.Testing;
using StackExchange.Redis;

namespace Unit.Owned;

public sealed class UnitRuntimeEdgeTests : IDisposable
{
    private const long Epoch = 5;
    private static readonly TimeSpan Wait = Bounds.Safety;

    private readonly SignallingTimeProvider time = new();
    private readonly ClusterOptions options = MembershipTestOptions.Default();
    private readonly LocalPhaseGate gate;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly ProbeUnitRows rows = new();
    private readonly RecordingUnit unit = new();
    private readonly ThrowingUnit thrower = new();
    private readonly List<UnitRuntime> runtimes = [];

    public UnitRuntimeEdgeTests()
    {
        options.LeaseTtl = TimeSpan.FromDays(1);
        gate = new LocalPhaseGate(Options.Create(options), time, new RecordingModelEvents(), NullLogger<LocalPhaseGate>.Instance);
        capabilities = new ClusterCapabilitiesRegistry(new RecordingCapabilityStore(), gate, Options.Create(options), time);
    }

    public void Dispose()
    {
        unit.ReleaseAll();
        foreach (var runtime in runtimes)
        {
            runtime.Dispose();
        }

        gate.Dispose();
    }

    [Fact]
    public async Task Rows_read_under_an_epoch_that_ended_during_the_read_are_not_started()
    {
        var runtime = Runtime();
        ToServing(Epoch);
        var read = new TaskCompletionSource<IReadOnlyList<HeldRow>>(TaskCreationOptions.RunContinuationsAsynchronously);
        rows.Read = (_, _) => read.Task;
        var stale = runtime.PassAsync(CancellationToken.None);
        Assert.Equal([Epoch], rows.ReadEpochs);

        Rejoin(Epoch + 1);
        read.SetResult([Row("a", 10)]);
        await stale.WaitAsync(Wait);

        rows.Read = (_, _) => Held(Row("a", 10));
        await runtime.PassAsync(CancellationToken.None);
        Assert.Equal(0, rows.Releases.Reader.Count);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("widget", "a", 10L), (started.Kind, started.Key, started.Token));
    }

    [Fact]
    public async Task A_throwing_OnAcquiredAsync_retries_with_capped_backoff_on_the_same_tenure()
    {
        var runtime = Runtime();
        ToServing(Epoch);
        rows.Read = (_, _) => Held(new HeldRow("thrower", "k", 10, false));
        await runtime.PassAsync(CancellationToken.None);
        var first = await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        var gaps = new[] { 1, 2, 4, 8, 16, 32, 60 };
        for (var i = 0; i < gaps.Length; i++)
        {
            await time.TimerAsync(TimeSpan.FromSeconds(gaps[i]));
            await runtime.PassAsync(CancellationToken.None);
            Assert.Equal(0, rows.Releases.Reader.Count);
            time.Advance(TimeSpan.FromSeconds(gaps[i]) - TimeSpan.FromTicks(1));
            Assert.Equal(i + 1, thrower.Starts);
            time.Advance(TimeSpan.FromTicks(1));
            var attempt = await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            Assert.Same(first, attempt);
        }

        await time.TimerAsync(TimeSpan.FromSeconds(60));
        time.Advance(TimeSpan.FromSeconds(60) - TimeSpan.FromTicks(1));
        Assert.Equal(8, thrower.Starts);
        time.Advance(TimeSpan.FromTicks(1));
        Assert.Same(first, await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait));
        Assert.Equal(9, thrower.Starts);

        rows.Read = (_, _) => Held(new HeldRow("thrower", "k", 10, true));
        await runtime.PassAsync(CancellationToken.None);
        await thrower.Lost.Task.WaitAsync(Wait);
        var release = await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("thrower", "k", Epoch, 10L), release);
        Assert.Equal(9, thrower.Starts);
        await time.TimerAsync(TimeSpan.FromSeconds(60));
        time.Advance(TimeSpan.FromHours(1));
        Assert.Equal(9, thrower.Starts);
    }

    [Fact]
    public async Task A_returning_OnAcquiredAsync_is_not_restarted()
    {
        var runtime = Runtime();
        ToServing(Epoch);
        rows.Read = (_, _) => Held(new HeldRow("thrower", "k", 10, false));
        thrower.ReturnNormally = true;
        await runtime.PassAsync(CancellationToken.None);
        await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        time.Advance(TimeSpan.FromHours(1));
        Assert.Equal(1, thrower.Starts);
    }

    [Fact]
    public async Task A_start_that_throws_once_then_waits_is_started_exactly_twice()
    {
        var runtime = Runtime();
        ToServing(Epoch);
        rows.Read = (_, _) => Held(new HeldRow("thrower", "k", 10, false));
        thrower.ThrowsTo = 1;
        await runtime.PassAsync(CancellationToken.None);
        var context = await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        await time.TimerAsync(TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Same(context, await thrower.Attempts.Reader.ReadAsync().AsTask().WaitAsync(Wait));
        time.Advance(TimeSpan.FromHours(1));
        Assert.Equal(2, thrower.Starts);

        rows.Read = (_, _) => Held(new HeldRow("thrower", "k", 10, true));
        await runtime.PassAsync(CancellationToken.None);
        await thrower.Lost.Task.WaitAsync(Wait);
        Assert.Equal(("thrower", "k", Epoch, 10L), await rows.Releases.Reader.ReadAsync().AsTask().WaitAsync(Wait));
        Assert.Equal(2, thrower.Starts);
    }

    [Fact]
    public async Task A_read_not_answered_within_its_bound_ends_the_pass_on_the_runtimes_clock()
    {
        options.FencedTransactionTimeout = TimeSpan.FromHours(1);
        var runtime = Runtime();
        ToServing(Epoch);
        rows.Read = async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return Array.Empty<HeldRow>();
        };

        using var cancel = new CancellationTokenSource();
        var pass = runtime.PassAsync(cancel.Token);
        try
        {
            time.Advance(options.FencedTransactionTimeout + TimeSpan.FromSeconds(3));
            await pass.WaitAsync(Wait);
        }
        finally
        {
            await cancel.CancelAsync();
        }
    }

    [Fact]
    public async Task RedisFence_install_raise_waits_until_FencedTransactionTimeout_then_starts_fenced_against_writes()
    {
        options.FencedTransactionTimeout = TimeSpan.FromSeconds(5);
        var redis = DispatchProxy.Create<IConnectionMultiplexer, RedisMultiplexerProxy>();
        var redisProxy = (RedisMultiplexerProxy)(object)redis;
        var db = DispatchProxy.Create<IDatabase, RedisDatabaseProxy>();
        var dbProxy = (RedisDatabaseProxy)(object)db;
        redisProxy.Database = db;

        var services = new ServiceCollection().AddSingleton(unit).BuildServiceProvider();
        var runtime = new UnitRuntime(
            [new UnitKind("widget", typeof(RecordingUnit), new UnitKindOptions { RedisFence = true })],
            services, rows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, wakeups,
            new RedisTenureFences(
                redis, gate, NoFaultPoints.Instance, Options.Create(new ClusterRedisOptions()), Options.Create(options), time,
                NullLogger<RedisTenureFences>.Instance),
            Options.Create(options), time, NullLogger<UnitRuntime>.Instance);
        runtimes.Add(runtime);
        ToServing(Epoch);
        rows.Read = (_, _) => Held(Row("a", 10));

        long acquiredAt = -1;
        unit.OnAcquiring = () => acquiredAt = time.GetTimestamp();
        var startedAt = time.GetTimestamp();
        var pass = runtime.PassAsync(CancellationToken.None);
        await dbProxy.RaiseStarted.Task.WaitAsync(Wait);
        time.Advance(options.FencedTransactionTimeout - TimeSpan.FromTicks(1));
        time.Advance(TimeSpan.FromTicks(1));
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(options.FencedTransactionTimeout, time.GetElapsedTime(startedAt, acquiredAt));
        Assert.Equal(("widget", "a", 10L), (started.Kind, started.Key, started.Token));
        await Assert.ThrowsAsync<RedisFenceRefusedException>(() =>
            started.Context.FenceRedisAsync(_ => { }));

        Assert.False(dbProxy.Raise.Task.IsCompleted);
        _ = pass;
    }

    [Fact]
    public async Task A_logger_that_throws_while_a_pass_decides_a_stop_does_not_lose_the_stop()
    {
        var runtime = Runtime(new ThrowingOnceLogger("is released at once"));
        ToServing(Epoch);
        rows.Read = (_, _) => Held(Row("a", 10));
        await runtime.PassAsync(CancellationToken.None);
        var started = await unit.Acquired.Reader.ReadAsync().AsTask().WaitAsync(Wait);

        rows.Read = (_, _) => Held(Row("a", 11));
        try
        {
            await runtime.PassAsync(CancellationToken.None);
        }
        catch (InvalidOperationException)
        {
        }

        Assert.True(started.Stop.IsCancellationRequested);

        await unit.LostReturned("a").WaitAsync(Wait);
    }

    [Fact]
    public void The_failover_bound_is_the_spec_sum_with_the_placer_dead_too_plus_the_start_time()
    {
        var kind = new UnitKind("widget", typeof(RecordingUnit), new UnitKindOptions { StartTime = TimeSpan.FromSeconds(4) });

        Assert.Equal(TimeSpan.FromSeconds(45), kind.FailoverBound(new ClusterOptions()));

        var fenced = new UnitKind("widget", typeof(RecordingUnit), new UnitKindOptions { StartTime = TimeSpan.FromSeconds(4), RedisFence = true });
        Assert.Equal(TimeSpan.FromSeconds(50), fenced.FailoverBound(new ClusterOptions()));
    }

    private static HeldRow Row(string key, long token, bool requested = false) => new("widget", key, token, requested);

    private static Task<IReadOnlyList<HeldRow>> Held(params HeldRow[] held) => Task.FromResult<IReadOnlyList<HeldRow>>(held);

    private UnitRuntime Runtime(ILogger<UnitRuntime>? logger = null)
    {
        var services = new ServiceCollection().AddSingleton(unit).AddSingleton(thrower).BuildServiceProvider();
        UnitKind[] registered =
        [
            new("widget", typeof(RecordingUnit), new UnitKindOptions()),
            new("thrower", typeof(ThrowingUnit), new UnitKindOptions()),
        ];
        var runtime = new UnitRuntime(
            registered, services, rows, gate, new ClusterScopeSource(new FenceSteps()), Unit.Generations.Stores.Create("config"),
            NoFaultPoints.Instance, capabilities, wakeups, new NoTenureFence(), Options.Create(options), time, logger ?? NullLogger<UnitRuntime>.Instance);
        runtimes.Add(runtime);
        return runtime;
    }

    private void ToServing(long epoch)
    {
        Assert.True(gate.Joined(epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
        Assert.Equal(epoch, gate.Epoch);
    }

    private void Rejoin(long epoch)
    {
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        gate.CompleteFence();
        Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
        gate.Apply(MemberTrigger.RejoinPermitted);
        ToServing(epoch);
    }

    private sealed class ProbeUnitRows : IUnitRows
    {
        private readonly ConcurrentQueue<long> readEpochs = new();

        public Func<long, CancellationToken, Task<IReadOnlyList<HeldRow>>> Read { get; set; } = (_, _) => Held();

        public IReadOnlyList<long> ReadEpochs => [.. readEpochs];

        public Channel<(string Kind, string Key, long Epoch, long Token)> Releases { get; } =
            Channel.CreateUnbounded<(string, string, long, long)>();

        public Task<IReadOnlyList<HeldRow>> ReadHeldAsync(string owner, long epoch, CancellationToken cancellationToken)
        {
            readEpochs.Enqueue(epoch);
            return Read(epoch, cancellationToken);
        }

        public Task<bool> ReleaseAsync(string kind, string key, string owner, long epoch, long token, CancellationToken cancellationToken)
        {
            Releases.Writer.TryWrite((kind, key, epoch, token));
            return Task.FromResult(true);
        }

        public Task<IReadOnlyList<OrphanedRow>> ReadOrphanedAsync(IReadOnlyList<string> kinds, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<OrphanedRow>>([]);

        public Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken) =>
            Task.FromResult<long?>(null);
    }

    private sealed class ThrowingUnit : IOwnedUnit
    {
        private int starts;

        public int Starts => Volatile.Read(ref starts);

        public TaskCompletionSource Threw { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Lost { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Channel<OwnedContext> Attempts { get; } = Channel.CreateUnbounded<OwnedContext>();

        public bool ReturnNormally { get; set; }

        public int ThrowsTo { get; set; } = int.MaxValue;

        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
        {
            Interlocked.Increment(ref starts);
            Attempts.Writer.TryWrite(context);
            Threw.TrySetResult();
            if (ReturnNormally)
            {
                return Task.CompletedTask;
            }
            if (Starts <= ThrowsTo)
            {
                throw new InvalidOperationException("the unit's start failed");
            }
            return Task.Delay(Timeout.Infinite, stop);
        }

        public Task OnLostAsync(OwnedContext context)
        {
            Lost.TrySetResult();
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingOnceLogger(string fragment) : ILogger<UnitRuntime>
    {
        private int thrown;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).Contains(fragment, StringComparison.Ordinal) && Interlocked.Exchange(ref thrown, 1) == 0)
            {
                throw new InvalidOperationException("a logging provider failed");
            }
        }
    }
}

public class RedisMultiplexerProxy : DispatchProxy
{
    public IDatabase Database { get; set; } = null!;

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
    {
        "GetDatabase" => Database,
        _ => throw new NotSupportedException($"Unexpected Redis multiplexer call: {targetMethod?.Name}"),
    };
}

public class RedisDatabaseProxy : DispatchProxy
{
    public TaskCompletionSource<RedisValue[]> Raise { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource RaiseStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        if (targetMethod?.Name == "StringGetAsync")
        {
            RaiseStarted.TrySetResult();
            return Raise.Task;
        }

        throw new NotSupportedException($"Unexpected Redis database call: {targetMethod?.Name}");
    }
}
