using System.Text;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Int.Primitives;

[Collection(RedisPolicyMetricCollection.Name)]
public sealed class RedisSingleFlightTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly RedisFixture redis;

    public RedisSingleFlightTests(RedisFixture redis) => this.redis = redis;

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public async Task I16_concurrent_callers_on_four_replicas_compute_once_and_the_rest_wait_for_the_same_result()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flights = new List<RedisSingleFlight>();
        foreach (var id in new[] { "a", "b", "c", "d" })
        {
            flights.Add((await rig.AddReplicaAsync(id)).Flight(OnRedisUnavailable.Refuse));
        }

        var computations = 0;
        var results = await Task.WhenAll(Enumerable.Range(0, 24).Select(i => flights[i % 4].RunAsync(
            "k",
            async ct =>
            {
                Interlocked.Increment(ref computations);
                await Task.Delay(300, ct);
                return Bytes("value");
            },
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMinutes(1))));

        Assert.Equal(1, computations);
        Assert.Single(results.OfType<SingleFlightResult.Computed>());
        Assert.Equal(23, results.OfType<SingleFlightResult.Waited>().Count());
        Assert.All(results, r => Assert.True(r.TryGetValue(out var value) && value!.SequenceEqual(Bytes("value"))));
    }

    [Fact]
    public async Task The_result_is_stored_with_its_ttl_under_the_claim_s_hash_tag_and_the_claim_ends_with_it()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flight = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);

        var result = await flight.RunAsync("k", _ => Task.FromResult(Bytes("v")), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));

        Assert.IsType<SingleFlightResult.Computed>(result);
        Assert.Equal($"{{{rig.Prefix}flight:k}}", RedisPrimitiveKeys.FlightHashTag(rig.Prefix, "k"));
        Assert.Equal("v", (string?)await rig.Direct.StringGetAsync(rig.FlightResultKey("k")));
        Assert.InRange((await rig.Direct.KeyTimeToLiveAsync(rig.FlightResultKey("k")))!.Value.TotalSeconds, 1, 60);
        Assert.False(await rig.Direct.KeyExistsAsync(rig.FlightTokenKey("k")));
        FlightAnswer.Is<SingleFlightResult.Waited>(
            Bytes("v"),
            await flight.RunAsync("k", _ => throw new InvalidOperationException("not computed"), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task While_it_computes_the_flight_is_one_token_key_that_carries_the_holder_and_a_waiter_on_another_replica_reads_the_result()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Flight(OnRedisUnavailable.Refuse);
        var started = new TaskCompletionSource();
        var finish = new TaskCompletionSource();
        var computing = a.RunAsync("k", async _ =>
        {
            started.SetResult();
            await finish.Task;
            return Bytes("v");
        }, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));
        await started.Task.WaitAsync(Wait);

        var token = (string?)await rig.Direct.StringGetAsync(rig.FlightTokenKey("k"));
        Assert.Matches("^[0-9a-f]{32}:a$", token);
        Assert.False(await rig.Direct.KeyExistsAsync(RedisPrimitiveKeys.FlightHashTag(rig.Prefix, "k") + ":holder"));
        Assert.False(await rig.Direct.KeyExistsAsync(rig.FlightResultKey("k")));
        var waiting = b.RunAsync("k", _ => throw new InvalidOperationException("not computed"), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));
        finish.SetResult();

        FlightAnswer.Is<SingleFlightResult.Computed>(Bytes("v"), await computing.WaitAsync(Wait));
        FlightAnswer.Is<SingleFlightResult.Waited>(Bytes("v"), await waiting.WaitAsync(Wait));
    }

    [Fact]
    public async Task A_grant_that_finds_a_live_result_hands_the_claim_key_back_and_answers_Waited()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flight = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);
        await flight.RunAsync("k", _ => Task.FromResult(Bytes("one")), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));
        var computations = 0;

        var answer = await flight.RunAsync("k", _ => { computations++; return Task.FromResult(Bytes("two")); }, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));

        FlightAnswer.Is<SingleFlightResult.Waited>(Bytes("one"), answer);
        Assert.Equal(0, computations);
        Assert.False(await rig.Direct.KeyExistsAsync(rig.FlightTokenKey("k")));
        Assert.Equal("one", (string?)await rig.Direct.StringGetAsync(rig.FlightResultKey("k")));
    }

    [Fact]
    public async Task Callers_on_a_key_whose_claim_expires_between_a_lost_grant_and_its_read_all_end_in_an_answer()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flights = new List<RedisSingleFlight>();
        foreach (var (id, policy) in new[] { ("a", OnRedisUnavailable.Refuse), ("b", OnRedisUnavailable.Grant), ("c", OnRedisUnavailable.Refuse), ("d", OnRedisUnavailable.Grant) })
        {
            flights.Add((await rig.AddReplicaAsync(id)).Flight(policy));
        }

        var calls = Enumerable.Range(0, 200).Select(i => flights[i % 4].RunAsync(
            "k", async ct => { await Task.Delay(2, ct); return Bytes("v"); }, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(1)));
        var results = await Task.WhenAll(calls).WaitAsync(Wait);

        Assert.All(results, r => Assert.True(r is SingleFlightResult.Computed or SingleFlightResult.ComputedRefused or SingleFlightResult.ComputedUnknown
            or SingleFlightResult.Waited or SingleFlightResult.Unknown));
    }

    [Fact]
    public async Task After_the_result_s_ttl_the_next_caller_computes_again()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flight = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);
        await flight.RunAsync("k", _ => Task.FromResult(Bytes("one")), TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(300));

        SingleFlightResult? again = null;
        await Poll.UntilAsync(
            async () => (again = await flight.RunAsync("k", _ => Task.FromResult(Bytes("two")), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1))) is SingleFlightResult.Computed,
            "the expired result is computed again");
        FlightAnswer.Is<SingleFlightResult.Computed>(Bytes("two"), again!);
    }

    [Fact]
    public async Task A_late_computation_s_result_is_refused_at_EXEC_and_the_successor_s_result_stands()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var a = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);
        var b = (await rig.AddReplicaAsync("b")).Flight(OnRedisUnavailable.Refuse);
        var slow = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var late = a.RunAsync("k", async _ =>
        {
            started.SetResult();
            await slow.Task;
            return Bytes("late");
        }, TimeSpan.FromMilliseconds(300), TimeSpan.FromMinutes(1));
        await started.Task.WaitAsync(Wait);
        await Poll.UntilAsync(async () => !await rig.Direct.KeyExistsAsync(rig.FlightTokenKey("k")), "a's claim expires");

        var successor = await b.RunAsync("k", _ => Task.FromResult(Bytes("fresh")), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1));
        slow.SetResult();

        FlightAnswer.Is<SingleFlightResult.Computed>(Bytes("fresh"), successor);
        FlightAnswer.Is<SingleFlightResult.ComputedRefused>(Bytes("late"), await late.WaitAsync(Wait));
        Assert.Equal("fresh", (string?)await rig.Direct.StringGetAsync(rig.FlightResultKey("k")));
    }

    [Fact]
    public async Task A_computation_that_throws_releases_its_claim_so_the_next_caller_computes_at_once()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var flight = (await rig.AddReplicaAsync("a")).Flight(OnRedisUnavailable.Refuse);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => flight.RunAsync("k", _ => throw new InvalidOperationException("boom"), TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)));

        Assert.False(await rig.Direct.KeyExistsAsync(rig.FlightTokenKey("k")));
        Assert.IsType<SingleFlightResult.Computed>(
            await flight.RunAsync("k", _ => Task.FromResult(Bytes("ok")), TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task A_replica_that_does_not_serve_computes_nothing()
    {
        await using var rig = await RedisPrimitivesRig.CreateAsync(redis);
        var replica = await rig.AddReplicaAsync("a");
        replica.Serving = false;

        var result = await replica.Flight(OnRedisUnavailable.Grant).RunAsync("k", _ => throw new InvalidOperationException("not computed"), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

        Assert.IsType<SingleFlightResult.NotServing>(result);
    }
}
