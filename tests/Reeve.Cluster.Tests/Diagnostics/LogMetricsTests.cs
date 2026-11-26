using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Log;
using Unit.Generations;
using Unit.Testing;
using Xunit;

namespace Unit.Diagnostics;

[Collection(MetricsCollection.Name)]
public sealed class LogMetricsTests
{
    [Fact]
    public async Task Generation_head_reports_the_head_of_the_current_generation_once_there_is_one()
    {
        var store = Stores.Create("gh-a");
        Assert.DoesNotContain(4_242_001, Observe<long>(ClusterDiagnostics.GenerationHead));

        await store.PublishAsync(4_242_001, null, ("gh-a", "s1"));
        Assert.Contains(4_242_001, Observe<long>(ClusterDiagnostics.GenerationHead));

        await store.PublishAsync(4_242_009, null, ("gh-a", "s2"));
        var heads = Observe<long>(ClusterDiagnostics.GenerationHead);
        Assert.Contains(4_242_009, heads);
        Assert.DoesNotContain(4_242_001, heads);
        GC.KeepAlive(store);
    }

    [Fact]
    public async Task Pins_count_the_scopes_holding_one_on_the_current_and_on_a_superseded_generation()
    {
        var store = Stores.Create("pin-a");
        var first = await store.PublishAsync(1, null, ("pin-a", "s1"));

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var (current0, superseded0) = Pins();
        var one = new ClusterScope(ClusterScopeKind.ClientRequest);
        var two = new ClusterScope(ClusterScopeKind.ClientRequest);

        one.Pin(first);
        AssertPins(current0 + 1, superseded0);

        var second = await store.PublishAsync(2, null, ("pin-a", "s2"));
        AssertPins(current0, superseded0 + 1);

        Assert.Same(second, two.PinOrGet(second));
        Assert.Same(second, two.PinOrGet(second));
        Assert.Same(first, one.PinOrGet(second));
        AssertPins(current0 + 1, superseded0 + 1);

        one.End();
        AssertPins(current0 + 1, superseded0);
        one.End();
        AssertPins(current0 + 1, superseded0);

        two.End();
        AssertPins(current0, superseded0);

        var late = new ClusterScope(ClusterScopeKind.ClientRequest);
        late.End();
        late.Pin(second);
        late.End();
        AssertPins(current0, superseded0);

        var foreign = new ClusterScope(ClusterScopeKind.ClientRequest);
        foreign.Pin(new OtherGeneration());
        foreign.End();
        AssertPins(current0, superseded0);
        GC.KeepAlive(store);

        (long Current, long Superseded) Pins() => (Sum("current"), Sum("superseded"));

        void AssertPins(long current, long superseded)
        {
            Assert.Equal(current, Sum("current"));
            Assert.Equal(superseded, Sum("superseded"));
        }

        static long Sum(string generation)
        {
            using var recorder = new InstrumentRecorder(ClusterDiagnostics.GenerationPins);
            recorder.RecordObservable();
            return (long)recorder.Measurements().Where(m => (string?)m.Tags["generation"] == generation).Sum(m => m.Value);
        }
    }

    [Fact]
    public async Task An_attempt_records_its_duration_by_outcome_and_pass()
    {
        var time = new FakeTimeProvider();
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.AttemptDuration);

        var accepted = new TaskCompletionSource<ReconcileResult>();
        var store = StoreOf(time, "at-accepted");
        var pass = store.BeginPass(1, null, ["at-accepted"]);
        var running = pass.RunAttemptAsync("at-accepted", _ => accepted.Task, TimeSpan.FromSeconds(30));
        time.Advance(TimeSpan.FromSeconds(2));
        accepted.SetResult(new ReconcileResult("s"));
        Assert.Equal(AttemptOutcomeKind.Accepted, (await running.WaitAsync(Bounds.Safety)).Kind);
        AssertOne(recorder, "at-accepted", 2, "accepted", "incremental");

        var failing = new TaskCompletionSource<ReconcileResult>();
        store = StoreOf(time, "at-failed");
        pass = store.BeginPass(1, null, ["at-failed"], full: true);
        running = pass.RunAttemptAsync("at-failed", _ => failing.Task, TimeSpan.FromSeconds(30));
        time.Advance(TimeSpan.FromSeconds(3));
        failing.SetException(new InvalidOperationException("boom"));
        Assert.Equal(AttemptOutcomeKind.Failed, (await running.WaitAsync(Bounds.Safety)).Kind);
        AssertOne(recorder, "at-failed", 3, "failed", "full");

        var hung = new TaskCompletionSource<ReconcileResult>();
        store = StoreOf(time, "at-timed-out");
        pass = store.BeginPass(1, null, ["at-timed-out"]);
        running = pass.RunAttemptAsync("at-timed-out", _ => hung.Task, TimeSpan.FromSeconds(1));
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(AttemptOutcomeKind.TimedOut, (await running.WaitAsync(Bounds.Safety)).Kind);
        AssertOne(recorder, "at-timed-out", 1, "timed_out", "incremental");
        hung.SetResult(new ReconcileResult("late"));
    }

    [Fact]
    public async Task An_attempt_the_pass_stopped_awaiting_records_nothing()
    {
        var time = new FakeTimeProvider();
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.AttemptDuration);
        var store = StoreOf(time, "at-abandoned");
        var pass = store.BeginPass(1, null, ["at-abandoned"]);
        var hung = new TaskCompletionSource<ReconcileResult>();
        var running = pass.RunAttemptAsync("at-abandoned", _ => hung.Task, TimeSpan.FromSeconds(30));

        time.Advance(TimeSpan.FromSeconds(4));
        pass.Abandon();

        Assert.Equal(AttemptOutcomeKind.Abandoned, (await running.WaitAsync(Bounds.Safety)).Kind);
        Assert.DoesNotContain(recorder.Measurements(), m => (string?)m.Tags["aggregate_type"] == "at-abandoned");
        hung.SetResult(new ReconcileResult("late"));
    }

    [Fact]
    public async Task Deferred_work_counts_and_ages_from_its_publication_until_it_returns()
    {
        var time = new FakeTimeProvider();
        var store = StoreOf(time, "df-a", "df-b");
        var release = new TaskCompletionSource();
        var pass = store.BeginPass(1, null, ["df-a", "df-b"]);
        await pass.AcceptAsync("df-a", new ReconcileResult("a") { Deferred = (_, _) => release.Task });
        await pass.AcceptAsync("df-b", new ReconcileResult("b"));

        Assert.NotNull(store.Publish(pass));
        AssertDeferred("df-a", 1, 0);
        AssertDeferred("df-b", 0, 0);

        time.Advance(TimeSpan.FromSeconds(5));
        AssertDeferred("df-a", 1, 5);

        release.SetResult();
        await UntilAsync(() => Running("df-a") == 0);
        AssertDeferred("df-a", 0, 0);
        GC.KeepAlive(store);
    }

    [Fact]
    public async Task Deferred_work_ages_from_the_oldest_of_a_types_runs()
    {
        var time = new FakeTimeProvider();
        var store = StoreOf(time, "df-oldest");
        var release = new TaskCompletionSource();

        var first = store.BeginPass(1, null, ["df-oldest"]);
        await first.AcceptAsync("df-oldest", new ReconcileResult("s1") { Deferred = (_, _) => release.Task });
        Assert.NotNull(store.Publish(first));
        time.Advance(TimeSpan.FromSeconds(5));
        var second = store.BeginPass(2, null, ["df-oldest"]);
        await second.AcceptAsync("df-oldest", new ReconcileResult("s2") { Deferred = (_, _) => release.Task });
        Assert.NotNull(store.Publish(second));
        time.Advance(TimeSpan.FromSeconds(2));

        Assert.Equal(2, Running("df-oldest"));
        AssertDeferred("df-oldest", 2, 7);

        release.SetResult();
        await UntilAsync(() => Running("df-oldest") == 0);
        GC.KeepAlive(store);
    }

    [Theory]
    [InlineData("throws")]
    [InlineData("cancelled")]
    public async Task Deferred_work_that_throws_or_is_cancelled_leaves_the_count(string how)
    {
        var type = "df-" + how;
        var store = StoreOf(new FakeTimeProvider(), type);
        var pass = store.BeginPass(1, null, [type]);
        await pass.AcceptAsync(type, new ReconcileResult("s")
        {
            Deferred = (_, token) => how == "throws" ? throw new InvalidOperationException("boom") : Task.FromCanceled(new CancellationToken(true)),
        });

        Assert.NotNull(store.Publish(pass));

        await UntilAsync(() => Running(type) == 0);
        AssertDeferred(type, 0, 0);
        GC.KeepAlive(store);
    }

    private static GenerationStore StoreOf(TimeProvider time, params string[] types) =>
        new(types, NoFaultPoints.Instance, NoModelEvents.Instance, time, NullLogger.Instance);

    private static void AssertOne(InstrumentRecorder recorder, string type, double seconds, string outcome, string pass)
    {
        var measurement = Assert.Single(recorder.Measurements(), m => (string?)m.Tags["aggregate_type"] == type);
        Assert.Equal(seconds, measurement.Value);
        Assert.Equal(outcome, measurement.Tags["outcome"]);
        Assert.Equal(pass, measurement.Tags["pass"]);
    }

    private static void AssertDeferred(string type, int running, double age)
    {
        Assert.Equal(running, Running(type));
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.DeferredOldestAge);
        recorder.RecordObservable();
        Assert.Equal(age, Assert.Single(recorder.Measurements(), m => (string?)m.Tags["aggregate_type"] == type).Value);
    }

    private static int Running(string type)
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.DeferredRunning);
        recorder.RecordObservable();
        return (int)Assert.Single(recorder.Measurements(), m => (string?)m.Tags["aggregate_type"] == type).Value;
    }

    private static List<T> Observe<T>(Instrument instrument)
        where T : struct
    {
        var values = new List<T>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (published, l) =>
        {
            if (ReferenceEquals(published, instrument))
            {
                l.EnableMeasurementEvents(published);
            }
        };
        listener.SetMeasurementEventCallback<T>((_, value, _, _) => values.Add(value));
        listener.Start();
        listener.RecordObservableInstruments();
        return values;
    }

    private static async Task UntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Bounds.Safety;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "The condition did not hold in time.");
            await Task.Delay(10);
        }
    }

    private sealed class OtherGeneration : IGeneration
    {
        public long Head => 1;

        public Guid? HeadToken => null;

        public object Get(string aggregateType) => throw new NotSupportedException();

        public CancellationToken Superseded(string aggregateType) => CancellationToken.None;

        public bool TryActivate(string aggregateType, Action activate) => throw new NotSupportedException();
    }
}
