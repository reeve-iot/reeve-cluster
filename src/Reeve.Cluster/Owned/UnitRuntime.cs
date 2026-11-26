using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Owned;

internal sealed class UnitRuntime : IHostedService, IDisposable, IRunningTenures, ICommandTenures
{
    private static readonly TimeSpan InitialStartRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaximumStartRetryDelay = TimeSpan.FromSeconds(60);

    private readonly Dictionary<string, (UnitKind Kind, IOwnedUnit Unit)> kinds = new(StringComparer.Ordinal);
    private readonly IUnitRows rows;
    private readonly ClusterCapabilitiesRegistry capabilities;
    private readonly LocalPhaseGate gate;
    private readonly IClusterScopeSource scopes;
    private readonly IGenerationSource generations;
    private readonly IFaultPoints points;
    private readonly ITenureFence redisFence;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly string replicaId;
    private readonly object sync = new();
    private readonly Dictionary<(string Kind, string Key), Tenure> tenures = [];
    private readonly IDisposable phases;
    private readonly IDisposable stepZero;
    private readonly IDisposable nudges;
    private readonly CancellationTokenSource stopping = new();
    private TaskCompletionSource wake = NewWake();
    private long servingEpoch;
    private long removals;
    private int polls;
    private Task? running;
    private int disposed;

    public UnitRuntime(
        IEnumerable<UnitKind> registered,
        IServiceProvider services,
        IUnitRows rows,
        LocalPhaseGate gate,
        IClusterScopeSource scopes,
        IGenerationSource generations,
        IFaultPoints points,
        ClusterCapabilitiesRegistry capabilities,
        IWakeupChannel wakeups,
        ITenureFence redisFence,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<UnitRuntime> logger)
    {
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(capabilities);
        ArgumentNullException.ThrowIfNull(wakeups);
        ArgumentNullException.ThrowIfNull(redisFence);
        ArgumentNullException.ThrowIfNull(options);
        this.rows = rows;
        this.capabilities = capabilities;
        this.gate = gate;
        this.scopes = scopes;
        this.generations = generations;
        this.points = points;
        this.redisFence = redisFence;
        this.options = options.Value;
        this.time = time;
        this.logger = logger;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));

        foreach (var kind in registered)
        {
            kinds.Add(kind.Kind, (kind, (IOwnedUnit)services.GetRequiredService(kind.UnitType)));
            capabilities.AddUnitKind(kind.Kind);
        }

        phases = gate.Subscribe(OnPhase);
        stepZero = gate.OnStepZero(_ => OnStepZero());
        ClusterDiagnostics.TrackRuntime(this);
        nudges = wakeups.Subscribe(WakeupKind.OwnerNudge, w =>
        {
            if (w.Target == replicaId)
            {
                Nudge();
            }
        });
    }

    public void Nudge() => Volatile.Read(ref wake).TrySetResult();

    internal bool NudgePending => Volatile.Read(ref wake).Task.IsCompleted;

    internal int Polls => Volatile.Read(ref polls);

    internal bool Holds(string kind, string key)
    {
        lock (sync)
        {
            return tenures.ContainsKey((kind, key));
        }
    }

    internal IReadOnlyList<HeldTenure> HeldTenures()
    {
        lock (sync)
        {
            return [.. tenures.Values.Select(t => new HeldTenure(t.Kind, t.Key, t.RowToken, t.Epoch, t.IsRunning, t.Ready))
                .OrderBy(t => t.Kind, StringComparer.Ordinal).ThenBy(t => t.Key, StringComparer.Ordinal)];
        }
    }

    internal IReadOnlyList<(string Kind, int Running)> RunningByKind()
    {
        lock (sync)
        {
            var running = kinds.Keys.ToDictionary(k => k, _ => 0, StringComparer.Ordinal);
            foreach (var tenure in tenures.Values.Where(t => t.IsRunning))
            {
                running[tenure.Kind] = running.GetValueOrDefault(tenure.Kind) + 1;
            }

            return [.. running.Select(p => (p.Key, p.Value))];
        }
    }

    public bool TryGetRunning(string kind, string key, long token, out CancellationToken stopped, [NotNullWhen(true)] out OwnedContext? owner)
    {
        lock (sync)
        {
            if (tenures.TryGetValue((kind, key), out var tenure) && tenure.IsReady && tenure.RowToken == token)
            {
                stopped = tenure.StopSource.Token;
                owner = tenure;
                return true;
            }
        }

        stopped = default;
        owner = null;
        return false;
    }

    public bool IsStopping(string kind, string key, long token)
    {
        lock (sync)
        {
            return tenures.TryGetValue((kind, key), out var tenure) && !tenure.IsRunning && tenure.RowToken == token;
        }
    }

    public event Action? TenureStarted;

    public IReadOnlyList<CommandTenure> RunningOf(IReadOnlyCollection<string> kinds)
    {
        ArgumentNullException.ThrowIfNull(kinds);
        var found = new List<CommandTenure>();
        lock (sync)
        {
            foreach (var tenure in tenures.Values)
            {
                if (tenure.IsReady && !tenure.Options.Convergent && kinds.Contains(tenure.Kind))
                {
                    found.Add(new CommandTenure(tenure.Kind, tenure.Key, tenure.RowToken, tenure, tenure, tenure.StopSource.Token));
                }
            }
        }

        return found;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (kinds.Count > 0)
        {
            Volatile.Write(ref running, Task.Run(() => PollAsync(stopping.Token), CancellationToken.None));
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);
        if (Volatile.Read(ref running) is { } loop)
        {
            await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        ClusterDiagnostics.UntrackRuntime(this);
        phases.Dispose();
        stepZero.Dispose();
        nudges.Dispose();
        stopping.Cancel();
    }

    internal async Task PassAsync(CancellationToken cancellationToken)
    {
        var epoch = Volatile.Read(ref servingEpoch);
        if (epoch == 0)
        {
            return;
        }

        await points.ReachAsync(OwnedFaultPoints.PollBeforeRead, cancellationToken).ConfigureAwait(false);
        await AdoptAsync(epoch, cancellationToken).ConfigureAwait(false);

        long removalsBefore;
        lock (sync)
        {
            removalsBefore = removals;
        }

        IReadOnlyList<HeldRow> held;
        long readAt;
        try
        {
            using var bound = Bound(cancellationToken);
            held = await rows.ReadHeldAsync(replicaId, epoch, bound.Token).ConfigureAwait(false);
            readAt = time.GetTimestamp();
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The own-row poll of epoch {Epoch} got no answer; the next poll reads again.", epoch);
            return;
        }

        var current = capabilities.Current;
        var releases = new List<HeldRow>();
        var starts = new List<HeldRow>();
        var stops = new List<(Tenure Tenure, bool Release)>();
        var logs = new List<Action>();
        bool stale;
        lock (sync)
        {
            stale = removals != removalsBefore;
            var seen = new HashSet<(string, string)>();
            foreach (var read in held)
            {
                var key = (read.Kind, read.Key);
                seen.Add(key);
                if (!kinds.TryGetValue(read.Kind, out var registered))
                {
                    logs.Add(() => logger.LogWarning("Kind {Kind} of key {Key} is not registered on this replica; the row is released.", read.Kind, read.Key));
                    releases.Add(read);
                    continue;
                }

                var ineligible = !registered.Kind.Options.Eligibility.IsEligible(current);
                var givenUp = ineligible && !registered.Kind.Keyed;
                var row = givenUp ? read with { ReleaseRequested = true } : read;
                if (tenures.TryGetValue(key, out var tenure))
                {
                    if (!tenure.IsRunning && tenure.LostEnded && (tenure.Epoch, tenure.RowToken) != (epoch, row.Token))
                    {
                        Remove(tenure);
                        (row.ReleaseRequested ? releases : starts).Add(row);
                    }
                    else
                    {
                        if (ineligible && registered.Kind.Keyed && tenure.IsRunning && !tenure.IneligibleWarned)
                        {
                            tenure.IneligibleWarned = true;
                            logs.Add(() => logger.LogWarning(
                                "{Kind}/{Key}: this replica no longer passes the keyed kind's eligibility filter, and keeps the key until it drains, fences, stops or crashes, or the resource is ended.",
                                read.Kind,
                                read.Key));
                        }

                        Judge(tenure, row, epoch, givenUp, releases, stops, logs);
                    }
                }
                else if (row.ReleaseRequested)
                {
                    if (givenUp)
                    {
                        logs.Add(() => logger.LogInformation(
                            "{Kind}/{Key} tenure {Token} is released without starting: this replica does not pass the kind's eligibility filter.",
                            read.Kind,
                            read.Key,
                            read.Token));
                    }

                    releases.Add(row);
                }
                else
                {
                    starts.Add(row);
                }
            }

            var ended = new List<Tenure>();
            foreach (var tenure in tenures.Values)
            {
                if (!tenure.IsRunning && tenure.LostEnded && !seen.Contains((tenure.Kind, tenure.Key)))
                {
                    ended.Add(tenure);
                }
                else if (tenure.IsRunning && !seen.Contains((tenure.Kind, tenure.Key)))
                {
                    var lost = tenure;
                    logs.Add(() => logger.LogWarning(
                        "The row of {Kind}/{Key} is no longer tenure {Token} of epoch {Epoch}; the unit is stopped as lost.",
                        lost.Kind,
                        lost.Key,
                        lost.RowToken,
                        lost.Epoch));
                    stops.Add((Stop(lost), false));
                }
            }

            foreach (var (tenure, _) in stops)
            {
                tenure.Finishing = true;
            }

            foreach (var tenure in ended)
            {
                Remove(tenure);
            }
        }

        foreach (var (tenure, release) in stops)
        {
            _ = FinishStopAsync(tenure, release);
        }

        foreach (var log in logs)
        {
            log();
        }

        foreach (var row in releases)
        {
            await ReleaseAsync(row.Kind, row.Key, epoch, row.Token, cancellationToken).ConfigureAwait(false);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        if (stale && starts.Count > 0)
        {
            Nudge();
            return;
        }

        foreach (var row in starts)
        {
            Start(row, epoch, readAt);
        }
    }

    private void Remove(Tenure tenure)
    {
        tenures.Remove((tenure.Kind, tenure.Key));
        removals++;
    }

    private async Task AdoptAsync(long epoch, CancellationToken cancellationToken)
    {
        var current = capabilities.Current;
        var adoptable = kinds.Values
            .Where(k => k.Kind.Options.Policy == AdoptionPolicy.AnyEligible && k.Kind.Options.Eligibility.IsEligible(current))
            .Select(k => k.Kind.Kind)
            .ToList();
        if (adoptable.Count == 0)
        {
            return;
        }

        IReadOnlyList<OrphanedRow> orphaned;
        try
        {
            using var bound = Bound(cancellationToken);
            orphaned = await rows.ReadOrphanedAsync(adoptable, bound.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(exception, "The adoption scan got no answer; the next poll reads again.");
            return;
        }

        foreach (var row in orphaned)
        {
            lock (sync)
            {
                if (tenures.ContainsKey((row.Kind, row.Key)))
                {
                    continue;
                }
            }

            if (cancellationToken.IsCancellationRequested || Volatile.Read(ref servingEpoch) != epoch || !gate.CheckServing().IsServing)
            {
                return;
            }

            try
            {
                using var bound = Bound(cancellationToken);
                if (await rows.TakeAsync(row.Kind, row.Key, row.Row, replicaId, epoch, bound.Token).ConfigureAwait(false) is { } token)
                {
                    logger.LogInformation(
                        "{Kind}/{Key} adopted from {Owner} epoch {OwnerEpoch} under tenure {Token}.", row.Kind, row.Key, row.Row.OwnerReplica, row.Row.OwnerEpoch, token);
                }
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogWarning(exception, "The adoption of {Kind}/{Key} got no answer; the next poll reads again.", row.Kind, row.Key);
            }
        }
    }

    private void Judge(Tenure tenure, HeldRow row, long epoch, bool givenUp, List<HeldRow> releases, List<(Tenure, bool)> stops, List<Action> logs)
    {
        if (tenure.Epoch == epoch && tenure.RowToken == row.Token)
        {
            if (tenure.IsRunning && row.ReleaseRequested)
            {
                logs.Add(givenUp
                    ? () => logger.LogInformation(
                        "{Kind}/{Key} tenure {Token}: this replica no longer passes the kind's eligibility filter; the unit is stopped and its row released.",
                        row.Kind,
                        row.Key,
                        row.Token)
                    : () => logger.LogInformation("A release of {Kind}/{Key} tenure {Token} is requested; the unit is stopped.", row.Kind, row.Key, row.Token));
                stops.Add((Stop(tenure), true));
            }
            else if (tenure.Release is { IsCompleted: true })
            {
                releases.Add(row);
            }

            return;
        }

        if (tenure.IsRunning)
        {
            var held = tenure.RowToken;
            logs.Add(() => logger.LogWarning(
                "{Kind}/{Key} is held under tenure {Token}, not {Held}; the unit is stopped as lost.", row.Kind, row.Key, row.Token, held));
            stops.Add((Stop(tenure), false));
        }

        logs.Add(() => logger.LogWarning(
            "{Kind}/{Key} tenure {Token} is released at once: the previous stop of the key has not ended.", row.Kind, row.Key, row.Token));
        releases.Add(row);
    }

    internal async Task StopAllAsync(bool drain, CancellationToken cancellationToken)
    {
        var begun = new List<(Tenure Tenure, bool Release)>();
        var ends = new List<Task>();
        var fenced = new List<Tenure>();
        lock (sync)
        {
            foreach (var tenure in tenures.Values)
            {
                if (!drain && tenure.Options.RedisFence)
                {
                    fenced.Add(tenure);
                }

                if (!tenure.Finishing)
                {
                    if (tenure.IsRunning)
                    {
                        Stop(tenure);
                    }

                    tenure.Finishing = true;
                    begun.Add((tenure, drain && !tenure.Options.Convergent && tenure.Options.Policy != AdoptionPolicy.AnyEligible));
                }

                ends.Add(tenure.Stopped.Task);
            }
        }

        foreach (var (tenure, release) in begun)
        {
            _ = FinishStopAsync(tenure, release);
        }

        ends.AddRange(fenced.Select(t => redisFence.OnFencedAsync(t, cancellationToken)));
        await Task.WhenAll(ends).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void OnStepZero()
    {
        lock (sync)
        {
            Interlocked.Exchange(ref servingEpoch, 0);
            foreach (var tenure in tenures.Values)
            {
                if (tenure.IsRunning)
                {
                    Stop(tenure);
                }
            }
        }
    }

    private static Tenure Stop(Tenure tenure)
    {
        tenure.IsRunning = false;
        _ = tenure.StopSource.CancelAsync();
        return tenure;
    }

    private void Start(HeldRow row, long epoch, long readAt)
    {
        if (!gate.CheckServing().IsServing)
        {
            return;
        }

        var (kind, unit) = kinds[row.Kind];
        var takenAt = readAt - (long)(row.HeldFor.TotalSeconds * time.TimestampFrequency);
        var tenure = new Tenure(this, kind, unit, row.Key, row.Token, epoch, takenAt);
        lock (sync)
        {
            if (gate.Phase != ReplicaPhase.Serving || Volatile.Read(ref servingEpoch) != epoch || !tenures.TryAdd((row.Kind, row.Key), tenure))
            {
                return;
            }
        }

        logger.LogInformation("{Kind}/{Key} tenure {Token} acquired; the unit starts.", row.Kind, row.Key, row.Token);
        _ = Task.Run(() => RunAcquiredAsync(tenure), CancellationToken.None);
    }

    private async Task RunAcquiredAsync(Tenure tenure)
    {
        try
        {
            if (tenure.Options.RedisFence)
            {
                await redisFence.OnAcquiringAsync(tenure, tenure.StopSource.Token).ConfigureAwait(false);
            }

            ClusterDiagnostics.TakeoverDuration.Record(time.GetElapsedTime(tenure.TakenAt).TotalSeconds, new KeyValuePair<string, object?>("kind", tenure.Kind));

            bool started;
            lock (sync)
            {
                tenure.Ready = true;
                started = tenure.IsRunning;
            }

            if (started)
            {
                TenureStarted?.Invoke();
            }

            var delay = InitialStartRetryDelay;
            while (true)
            {
                try
                {
                    await tenure.Unit.OnAcquiredAsync(tenure, tenure.StopSource.Token).ConfigureAwait(false);
                    break;
                }
                catch (OperationCanceledException) when (tenure.StopSource.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    if (tenure.StopSource.IsCancellationRequested)
                    {
                        logger.LogError(exception, "OnAcquiredAsync of {Kind}/{Key} tenure {Token} threw while stopping.", tenure.Kind, tenure.Key, tenure.RowToken);
                        break;
                    }

                    logger.LogError(exception, "OnAcquiredAsync of {Kind}/{Key} tenure {Token} threw; it runs again in {Delay}.", tenure.Kind, tenure.Key, tenure.RowToken, delay);

                    try
                    {
                        await Task.Delay(delay, time, tenure.StopSource.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (tenure.StopSource.IsCancellationRequested)
                    {
                        break;
                    }

                    if (tenure.StopSource.IsCancellationRequested)
                    {
                        break;
                    }

                    delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, MaximumStartRetryDelay.TotalSeconds));
                }
            }
        }
        catch (OperationCanceledException) when (tenure.StopSource.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogError(exception, "Starting {Kind}/{Key} tenure {Token} failed.", tenure.Kind, tenure.Key, tenure.RowToken);
        }
    }

    private async Task FinishStopAsync(Tenure tenure, bool release)
    {
        Task lost;
        using (ExecutionContext.SuppressFlow())
        {
            lost = Task.Run(() => tenure.Unit.OnLostAsync(tenure), CancellationToken.None);
        }

        try
        {
            await lost.WaitAsync(options.StopBudget, time).ConfigureAwait(false);
        }
        catch (TimeoutException) when (!lost.IsCompleted)
        {
            logger.LogWarning(
                "OnLostAsync of {Kind}/{Key} tenure {Token} did not return within StopBudget; the key does not start here until it does.",
                tenure.Kind,
                tenure.Key,
                tenure.RowToken);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogError(exception, "OnLostAsync of {Kind}/{Key} tenure {Token} threw.", tenure.Kind, tenure.Key, tenure.RowToken);
        }

        var answered = true;
        if (release)
        {
            tenure.Release = ReleaseAsync(tenure.Kind, tenure.Key, tenure.Epoch, tenure.RowToken, CancellationToken.None);
            answered = await tenure.Release.ConfigureAwait(false);
        }

        tenure.Stopped.TrySetResult();
        try
        {
            await lost.ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
        }

        lock (sync)
        {
            tenure.LostEnded = true;
            if (answered && tenures.TryGetValue((tenure.Kind, tenure.Key), out var current) && ReferenceEquals(current, tenure))
            {
                Remove(tenure);
            }
        }
    }

    private async Task<bool> ReleaseAsync(string kind, string key, long epoch, long token, CancellationToken cancellationToken)
    {
        try
        {
            using var bound = Bound(cancellationToken);
            if (!await rows.ReleaseAsync(kind, key, replicaId, epoch, token, bound.Token).ConfigureAwait(false))
            {
                logger.LogInformation("The release of {Kind}/{Key} tenure {Token} found the row no longer this tenure's.", kind, key, token);
            }

            return true;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "The release of {Kind}/{Key} tenure {Token} got no answer; a later poll releases it again.", kind, key, token);
            return false;
        }
    }

    private Bounded Bound(CancellationToken cancellationToken)
    {
        var timer = new CancellationTokenSource(options.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);
        return new Bounded(timer, CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token));
    }

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var next = NewWake();
            Interlocked.Exchange(ref wake, next);
            try
            {
                await PassAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogError(exception, "The own-row poll failed; the next poll runs again.");
            }

            Interlocked.Increment(ref polls);

            try
            {
                await next.Task.WaitAsync(options.PlacementInterval, time, cancellationToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private void OnPhase(PhaseEvent change)
    {
        if (change.Kind != PhaseEventKind.Transition)
        {
            return;
        }

        if (change.To == ReplicaPhase.Serving)
        {
            Interlocked.Exchange(ref servingEpoch, gate.Epoch ?? 0);
            Nudge();
        }
        else if (change.From == ReplicaPhase.Serving)
        {
            Interlocked.Exchange(ref servingEpoch, 0);
        }
    }

    private sealed class Bounded(CancellationTokenSource timer, CancellationTokenSource linked) : IDisposable
    {
        public CancellationToken Token => linked.Token;

        public void Dispose()
        {
            linked.Dispose();
            timer.Dispose();
        }
    }

    private static TaskCompletionSource NewWake() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class Tenure(UnitRuntime runtime, UnitKind kind, IOwnedUnit unit, string key, long token, long epoch, long takenAt) : OwnedContext, IFencedOwner
    {
        public override string Kind => kind.Kind;

        public override string Key => key;

        public override long Token => kind.Options.Convergent
            ? throw new InvalidOperationException($"Kind {kind.Kind} is convergent: a convergent unit gets no token.")
            : token;

        public long RowToken => token;

        public long Epoch => epoch;

        public long TakenAt => takenAt;

        public UnitKindOptions Options => kind.Options;

        public IOwnedUnit Unit => unit;

        public CancellationTokenSource StopSource { get; } = new();

        public bool IsRunning { get; set; } = true;

        public bool Ready { get; set; }

        public bool IsReady => Ready && IsRunning;

        public bool Finishing { get; set; }

        public Task<bool>? Release { get; set; }

        public bool LostEnded { get; set; }

        public bool IneligibleWarned { get; set; }

        public TaskCompletionSource Stopped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override IGeneration Configuration
        {
            get
            {
                var scope = runtime.scopes.CurrentWriter.UnlessEnded()
                    ?? throw new InvalidOperationException("Configuration is read inside a unit of work: begin one with IClusterScopeAccessor.BeginUnitOfWork().");
                if (scope.PinnedGeneration is { } pinned)
                {
                    return pinned;
                }

                runtime.generations.EnsureNotInsideTheSwap();
                var current = runtime.generations.Current
                    ?? throw new InvalidOperationException("No generation is published yet: the first pass has not completed.");
                CheckOwner();
                return scope.PinOrGet(current);
            }
        }

        public override void EnsureOwner()
        {
            if (kind.Options.Convergent)
            {
                throw new InvalidOperationException($"Kind {kind.Kind} is convergent: a convergent unit has no tenure to check.");
            }

            CheckOwner();

            var reached = runtime.points.ReachAsync(OwnedFaultPoints.EnsureOwnerBeforeEffect);
            if (!reached.IsCompletedSuccessfully)
            {
                reached.AsTask().GetAwaiter().GetResult();
            }
        }

        public IFaultPoints FaultPoints => runtime.points;

        public FencedStart Start()
        {
            EnsureOwner();
            if (!runtime.gate.TryGetTimeline(epoch, out var timeline))
            {
                throw new OwnershipLostException($"The join of epoch {epoch} has ended: {kind.Kind}/{key} tenure {token} may not act.");
            }

            var milliseconds = RecordTimeout.FromRemaining(runtime.options.FencedTransactionTimeout).Milliseconds
                ?? throw new InvalidOperationException("FencedTransactionTimeout must be positive.");
            return new FencedStart(kind.Kind, key, token, timeline, milliseconds);
        }

        public void OnOtherTimeline() => runtime.gate.OnOtherTimeline(epoch);

        public override async Task EndAsync(CancellationToken cancellationToken = default)
        {
            if (!kind.Keyed)
            {
                throw new InvalidOperationException($"Kind {kind.Kind} is not a keyed resource: a unit's row is not ended, and no creator would take it back.");
            }

            lock (runtime.sync)
            {
                if (!IsRunning)
                {
                    throw new OwnershipLostException($"{kind.Kind}/{key} tenure {token} is being stopped on this replica.");
                }

                Stop(this);
                Finishing = true;
            }

            bool ended;
            try
            {
                using var bound = runtime.Bound(cancellationToken == StopSource.Token ? CancellationToken.None : cancellationToken);
                ended = await runtime.rows.ReleaseAsync(kind.Kind, key, runtime.replicaId, epoch, token, bound.Token).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                _ = runtime.FinishStopAsync(this, release: true);
                throw;
            }

            _ = runtime.FinishStopAsync(this, release: false);
            if (!ended)
            {
                throw new OwnershipLostException($"The row of {kind.Kind}/{key} is no longer tenure {token}: nothing was ended.");
            }

            runtime.logger.LogInformation("{Kind}/{Key} tenure {Token} ended by its unit; the row stays as the key's tombstone.", kind.Kind, key, token);
        }

        private void CheckOwner()
        {
            if (StopSource.IsCancellationRequested)
            {
                throw new OwnershipLostException($"{kind.Kind}/{key} tenure {token} is being stopped on this replica.");
            }

            var check = runtime.gate.CheckServing();
            if (!check.IsServing)
            {
                throw new OwnershipLostException($"The replica is not serving ({check.Outcome}, phase {check.Phase}): {kind.Kind}/{key} tenure {token} may not act.");
            }
        }
    }
}

internal sealed class UnitFenceParticipant(UnitRuntime runtime) : IFenceParticipant
{
    public FenceStep Step => FenceStep.StopUnits;

    public ValueTask RunAsync(FenceContext context, CancellationToken cancellationToken) =>
        new(runtime.StopAllAsync(drain: false, cancellationToken));
}

internal sealed class UnitDrainParticipant(UnitRuntime runtime) : IDrainParticipant
{
    public DrainStep Step => DrainStep.ReleaseUnits;

    public ValueTask RunAsync(DrainContext context, CancellationToken cancellationToken) =>
        new(runtime.StopAllAsync(drain: true, cancellationToken));
}
