using System.Diagnostics.Metrics;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using Reeve.Cluster.Slots;
using StackExchange.Redis;

namespace Unit.Diagnostics;

[Collection(MetricsCollection.Name)]
public sealed class SlotMetricsTests
{
    private static readonly Guid Token = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    public static IEnumerable<object[]> Stores() => [[SlotStores.PostgreSql], [SlotStores.Redis]];

    private static void Required(ClusterOptions o)
    {
        o.ReplicaId = "replica-1";
        o.ConnectionString = "Host=db;Database=app";
        o.ReconcileTimeout = TimeSpan.FromSeconds(10);
    }

    private static string Tag(IReadOnlyDictionary<string, object?> tags, string key) => (string)tags[key]!;

    [Fact]
    public void The_instruments_are_named_and_typed_as_the_spec_lists_them()
    {
        Assert.Equal(("reeve_cluster.slot.acquires", "{acquire}"), (ClusterDiagnostics.SlotAcquires.Name, ClusterDiagnostics.SlotAcquires.Unit));
        Assert.Equal(("reeve_cluster.slot.renewals", "{renewal}"), (ClusterDiagnostics.SlotRenewals.Name, ClusterDiagnostics.SlotRenewals.Unit));
        Assert.Equal(("reeve_cluster.slot.held", "{slot}"), (ClusterDiagnostics.SlotHeld.Name, ClusterDiagnostics.SlotHeld.Unit));
        Assert.Equal("reeve_cluster.redis.policy_uses", RedisDiagnostics.PolicyUses.Name);
        Assert.Equal("slot", RedisDiagnostics.Slot);
        Assert.All(
            new Instrument[] { ClusterDiagnostics.SlotAcquires, ClusterDiagnostics.SlotRenewals, ClusterDiagnostics.SlotHeld },
            instrument => Assert.Same(ClusterDiagnostics.Name, instrument.Meter.Name));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Each_answer_of_an_acquire_is_one_measurement_tagged_with_the_store_and_the_result(string store)
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.SlotAcquires);
        var observer = new SlotMetricsObserver(store);

        observer.Acquired(new SlotAcquireResult.Acquired(Token));
        observer.Acquired(new SlotAcquireResult.Full());
        observer.Acquired(new SlotAcquireResult.NotServing());
        observer.Acquired(new SlotAcquireResult.Unknown());

        Assert.Equal(
            [(store, "acquired", 1d), (store, "full", 1d), (store, "not_serving", 1d), (store, "unknown", 1d)],
            recorder.Measurements().Select(m => (Tag(m.Tags, "store"), Tag(m.Tags, "result"), m.Value)));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Each_answer_of_a_renewal_is_one_measurement_tagged_with_the_store_and_the_result(string store)
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.SlotRenewals);
        var observer = new SlotMetricsObserver(store);

        observer.Renewed(SlotRenewResult.Renewed);
        observer.Renewed(SlotRenewResult.Ended);
        observer.Renewed(SlotRenewResult.NotServing);
        observer.Renewed(SlotRenewResult.Unknown);

        Assert.Equal(
            [(store, "renewed", 1d), (store, "ended", 1d), (store, "not_serving", 1d), (store, "unknown", 1d)],
            recorder.Measurements().Select(m => (Tag(m.Tags, "store"), Tag(m.Tags, "result"), m.Value)));
    }

    [Fact]
    public void An_acquire_is_not_counted_as_a_renewal_and_a_renewal_not_as_an_acquire()
    {
        using var acquires = new InstrumentRecorder(ClusterDiagnostics.SlotAcquires);
        using var renewals = new InstrumentRecorder(ClusterDiagnostics.SlotRenewals);

        new SlotMetricsObserver(SlotStores.Redis).Renewed(SlotRenewResult.Renewed);

        Assert.Empty(acquires.Measurements());
        Assert.Single(renewals.Measurements());
    }

    [Fact]
    public void The_gauge_counts_the_entries_of_each_stores_registries_whose_local_deadline_has_not_passed()
    {
        var time = new FakeTimeProvider();
        var postgres = new SlotRegistry(time);
        var redis = new SlotRegistry(time);
        var otherRedis = new SlotRegistry(time);
        var ttl = TimeSpan.FromSeconds(10);
        postgres.Record("k", "a", Token, 1, time.GetTimestamp(), ttl);
        redis.Record("k", "a", Token, 1, time.GetTimestamp(), ttl);
        redis.Record("k", "b", Token, 1, time.GetTimestamp(), TimeSpan.FromSeconds(20));
        otherRedis.Record("j", "a", Token, 1, time.GetTimestamp(), ttl);
        ClusterDiagnostics.TrackSlots(postgres, SlotStores.PostgreSql);
        ClusterDiagnostics.TrackSlots(redis, SlotStores.Redis);
        ClusterDiagnostics.TrackSlots(otherRedis, SlotStores.Redis);
        try
        {
            using var recorder = new InstrumentRecorder(ClusterDiagnostics.SlotHeld);
            recorder.RecordObservable();
            Assert.Equal(1, Held(recorder, SlotStores.PostgreSql));
            Assert.Equal(3, Held(recorder, SlotStores.Redis));

            time.Advance(ttl);
            using var later = new InstrumentRecorder(ClusterDiagnostics.SlotHeld);
            later.RecordObservable();
            Assert.Equal(0, Held(later, SlotStores.PostgreSql));
            Assert.Equal(1, Held(later, SlotStores.Redis));
        }
        finally
        {
            ClusterDiagnostics.UntrackSlots(postgres);
            ClusterDiagnostics.UntrackSlots(redis);
            ClusterDiagnostics.UntrackSlots(otherRedis);
        }
    }

    [Fact]
    public void A_registry_that_is_untracked_is_no_longer_counted()
    {
        var time = new FakeTimeProvider();
        var registry = new SlotRegistry(time);
        registry.Record("k", "a", Token, 1, time.GetTimestamp(), TimeSpan.FromSeconds(10));
        ClusterDiagnostics.TrackSlots(registry, "metrics-test-store");
        ClusterDiagnostics.UntrackSlots(registry);

        using var recorder = new InstrumentRecorder(ClusterDiagnostics.SlotHeld);
        recorder.RecordObservable();

        Assert.DoesNotContain(recorder.Measurements(), m => Tag(m.Tags, "store") == "metrics-test-store");
    }

    private static double Held(InstrumentRecorder recorder, string store) =>
        recorder.Measurements().Where(m => Tag(m.Tags, "store") == store).Sum(m => m.Value);

    [Fact]
    public async Task The_PostgreSQL_slots_AddReeveCluster_registers_count_their_answers_and_their_registry_in_the_gauge()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        await using var provider = services.BuildServiceProvider();
        using var acquires = new InstrumentRecorder(ClusterDiagnostics.SlotAcquires);
        using var renewals = new InstrumentRecorder(ClusterDiagnostics.SlotRenewals);
        using var held = new InstrumentRecorder(ClusterDiagnostics.SlotHeld);
        var slots = provider.GetRequiredService<IClusterSlots>();

        Assert.IsType<SlotAcquireResult.NotServing>(await slots.AcquireAsync("k", "i", 1, TimeSpan.FromSeconds(30)));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", Token, TimeSpan.FromSeconds(30)));
        held.RecordObservable();

        var acquire = Assert.Single(acquires.Measurements());
        Assert.Equal((SlotStores.PostgreSql, "not_serving"), (Tag(acquire.Tags, "store"), Tag(acquire.Tags, "result")));
        var renewal = Assert.Single(renewals.Measurements());
        Assert.Equal((SlotStores.PostgreSql, "ended"), (Tag(renewal.Tags, "store"), Tag(renewal.Tags, "result")));
        Assert.Contains(held.Measurements(), m => Tag(m.Tags, "store") == SlotStores.PostgreSql);
    }

    [Fact]
    public async Task The_Redis_slots_count_their_answers_under_the_redis_store_and_their_registry_in_the_gauge()
    {
        var services = new ServiceCollection().AddLogging().AddReeveCluster(new ClusterPorts([], typeof(NoFenceSink)), Required);
        services.AddSingleton(DispatchProxy.Create<IConnectionMultiplexer, NullMultiplexer>());
        services.AddReeveClusterRedisSlots(OnRedisUnavailable.Refuse);
        await using var provider = services.BuildServiceProvider();
        using var acquires = new InstrumentRecorder(ClusterDiagnostics.SlotAcquires);
        using var renewals = new InstrumentRecorder(ClusterDiagnostics.SlotRenewals);
        using var held = new InstrumentRecorder(ClusterDiagnostics.SlotHeld);
        var slots = provider.GetRequiredService<IRedisSlots>();

        Assert.IsType<SlotAcquireResult.NotServing>(await slots.AcquireAsync("k", "i", 1, TimeSpan.FromSeconds(30)));
        Assert.Equal(SlotRenewResult.Ended, await slots.RenewAsync("k", "i", Token, TimeSpan.FromSeconds(30)));
        held.RecordObservable();

        Assert.Equal(SlotStores.Redis, Tag(Assert.Single(acquires.Measurements()).Tags, "store"));
        Assert.Equal(SlotStores.Redis, Tag(Assert.Single(renewals.Measurements()).Tags, "store"));
        Assert.Contains(held.Measurements(), m => Tag(m.Tags, "store") == SlotStores.Redis);
    }

    public class NullMultiplexer : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => null;
    }
}
