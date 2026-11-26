using System.Reflection;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Unit.Redis;

public sealed class RedisClaimStoreTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    private readonly FakeRedis redis = new();
    private readonly Guid token = Guid.NewGuid();

    private RedisClaimStore Store() => new(redis.Database, "p:");

    private string Value(Guid held, string holder) => held.ToString("N") + ":" + holder;

    private string Watched(string space, Guid held, string holder) => $"WHEN {{p:{space}:k}}:token == {Value(held, holder)}";

    [Fact]
    public async Task A_claim_grant_is_one_native_SET_NX_PX_with_the_token_and_the_holder_in_the_value_and_opens_no_transaction()
    {
        Assert.True(await Store().GrantAsync(RedisSpace.Claim, "k", "r1", token, Ttl, CancellationToken.None));

        var set = Assert.Single(redis.Sent);
        Assert.Equal("SET {p:claim:k}:token", set.Command);
        Assert.Equal(Value(token, "r1"), set.Value);
        Assert.Equal((Ttl, When.NotExists), (set.Expiry, set.When));
    }

    [Fact]
    public async Task A_single_flight_grant_is_the_same_native_command_on_its_own_key_and_reads_no_result_key()
    {
        Assert.True(await Store().GrantAsync(RedisSpace.Flight, "k", "r1", token, Ttl, CancellationToken.None));

        var set = Assert.Single(redis.Sent);
        Assert.Equal("SET {p:flight:k}:token", set.Command);
        Assert.Equal(Value(token, "r1"), set.Value);
        Assert.Equal((Ttl, When.NotExists), (set.Expiry, set.When));
    }

    [Fact]
    public async Task A_grant_whose_key_exists_answers_false_and_one_Redis_did_not_answer_throws_and_is_never_false()
    {
        redis.SetAnswer = Task.FromResult(false);
        Assert.False(await Store().GrantAsync(RedisSpace.Claim, "k", "r1", token, Ttl, CancellationToken.None));

        redis.SetAnswer = Task.FromException<bool>(RedisFailures.Timeout());
        await Assert.ThrowsAsync<RedisTimeoutException>(() => Store().GrantAsync(RedisSpace.Claim, "k", "r1", token, Ttl, CancellationToken.None));
    }

    [Theory]
    [InlineData("r2")]
    [InlineData("")]
    [InlineData("a:b:c")]
    public async Task A_claim_read_names_the_holder_from_the_value_of_the_token_key_alone(string holder)
    {
        redis.Strings["{p:claim:k}:token"] = Value(token, holder);

        var record = await Store().ReadAsync(RedisSpace.Claim, "k", CancellationToken.None);

        Assert.Equal(new RedisRecord(true, holder, null), record);
        Assert.All(redis.Sent, sent => Assert.Equal("GET {p:claim:k}:token", sent.Command));
    }

    [Fact]
    public async Task A_token_key_that_0_3_0_wrote_is_present_with_no_holder_and_is_held_by_no_token()
    {
        redis.Strings["{p:claim:k}:token"] = token.ToString("N");

        Assert.Equal(new RedisRecord(true, string.Empty, null), await Store().ReadAsync(RedisSpace.Claim, "k", CancellationToken.None));
        Assert.False(await Store().ReleaseAsync(RedisSpace.Claim, "k", token, CancellationToken.None));
    }

    [Fact]
    public async Task A_claim_read_that_finds_no_key_finds_no_token()
    {
        var record = await Store().ReadAsync(RedisSpace.Claim, "k", CancellationToken.None);

        Assert.False(record.TokenPresent);
        Assert.Null(record.Outcome);
    }

    [Fact]
    public async Task A_single_flight_read_returns_the_holder_and_the_result_of_its_two_keys()
    {
        redis.Strings["{p:flight:k}:token"] = Value(token, "r2");
        redis.Strings["{p:flight:k}:result"] = "v";

        var record = await Store().ReadAsync(RedisSpace.Flight, "k", CancellationToken.None);

        Assert.True(record.TokenPresent);
        Assert.Equal("r2", record.Holder);
        Assert.Equal("v"u8.ToArray(), record.Outcome);
        Assert.Equal(["GET {p:flight:k}:token", "GET {p:flight:k}:result"], redis.Sent.Select(s => s.Command));
    }

    [Fact]
    public async Task A_release_of_the_held_token_deletes_the_key_in_a_transaction_conditioned_on_the_value_it_read()
    {
        redis.Strings["{p:claim:k}:token"] = Value(token, "r1");

        Assert.True(await Store().ReleaseAsync(RedisSpace.Claim, "k", token, CancellationToken.None));

        Assert.Equal(["GET {p:claim:k}:token", "MULTI", Watched("claim", token, "r1"), "DEL {p:claim:k}:token", "EXEC"], redis.Sent.Select(s => s.Command));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_release_of_a_token_the_key_does_not_hold_deletes_nothing(bool keyExists)
    {
        if (keyExists)
        {
            redis.Strings["{p:claim:k}:token"] = Value(Guid.NewGuid(), "r2");
        }

        Assert.False(await Store().ReleaseAsync(RedisSpace.Claim, "k", token, CancellationToken.None));

        Assert.Equal(["GET {p:claim:k}:token"], redis.Sent.Select(s => s.Command));
    }

    [Fact]
    public async Task An_effect_under_the_held_token_queues_its_commands_in_a_transaction_conditioned_on_the_value_it_read()
    {
        redis.Strings["{p:claim:k}:token"] = Value(token, "r1");
        var queued = 0;

        Assert.True(await Store().ExecuteAsync(RedisSpace.Claim, "k", token, _ => queued++, CancellationToken.None));

        Assert.Equal(1, queued);
        Assert.Equal(["GET {p:claim:k}:token", "MULTI", Watched("claim", token, "r1"), "EXEC"], redis.Sent.Select(s => s.Command));
    }

    [Fact]
    public async Task An_effect_under_a_token_the_key_does_not_hold_is_refused_and_queues_and_sends_nothing()
    {
        redis.Strings["{p:claim:k}:token"] = Value(Guid.NewGuid(), "r2");
        var queued = 0;

        Assert.False(await Store().ExecuteAsync(RedisSpace.Claim, "k", token, _ => queued++, CancellationToken.None));

        Assert.Equal(0, queued);
        Assert.Equal(["GET {p:claim:k}:token"], redis.Sent.Select(s => s.Command));
    }

    [Fact]
    public async Task A_result_write_under_the_held_token_sets_the_result_and_deletes_the_token_key_and_one_under_another_token_writes_nothing()
    {
        redis.Strings["{p:flight:k}:token"] = Value(token, "r1");

        Assert.True(await Store().CompleteAsync(RedisSpace.Flight, "k", token, [1], Ttl, CancellationToken.None));
        Assert.Equal(
            ["GET {p:flight:k}:token", "MULTI", Watched("flight", token, "r1"), "SET {p:flight:k}:result", "DEL {p:flight:k}:token", "EXEC"],
            redis.Sent.Select(s => s.Command));

        redis.Sent.Clear();
        Assert.False(await Store().CompleteAsync(RedisSpace.Flight, "k", Guid.NewGuid(), [1], Ttl, CancellationToken.None));
        Assert.Equal(["GET {p:flight:k}:token"], redis.Sent.Select(s => s.Command));
    }

    private sealed record Wire(string Command, string? Value = null, TimeSpan? Expiry = null, When? When = null);

    private sealed class FakeRedis
    {
        private readonly IDatabase database;
        private readonly ITransaction transaction;

        public FakeRedis()
        {
            database = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)(object)database).Owner = this;
            transaction = DispatchProxy.Create<ITransaction, Proxy>();
            ((Proxy)(object)transaction).Owner = this;
        }

        public IDatabase Database => database;

        public Dictionary<string, string> Strings { get; } = [];

        public List<Wire> Sent { get; } = [];

        public Task<bool> SetAnswer { get; set; } = Task.FromResult(true);

        private object? Handle(string name, object?[] args)
        {
            switch (name)
            {
                case "StringSetAsync" when args[0] is RedisKey key:
                    var expiry = args.OfType<TimeSpan>().Cast<TimeSpan?>().FirstOrDefault();
                    var when = args.OfType<When>().Cast<When?>().FirstOrDefault();
                    Sent.Add(new Wire($"SET {key}", (string?)(RedisValue)args[1]!, expiry, when));
                    return SetAnswer;
                case "StringGetAsync" when args[0] is RedisKey key:
                    Sent.Add(new Wire($"GET {key}"));
                    return Task.FromResult((RedisValue)(Strings.TryGetValue(key!, out var one) ? one : RedisValue.Null));
                case "StringGetAsync" when args[0] is RedisKey[] keys:
                    Sent.AddRange(keys.Select(k => new Wire($"GET {k}")));
                    return Task.FromResult(keys.Select(k => (RedisValue)(Strings.TryGetValue(k!, out var many) ? many : RedisValue.Null)).ToArray());
                case "CreateTransaction":
                    Sent.Add(new Wire("MULTI"));
                    return transaction;
                case "AddCondition":
                    Sent.Add(new Wire("WHEN " + args[0]));
                    return null;
                case "KeyDeleteAsync" when args[0] is RedisKey key:
                    Sent.Add(new Wire($"DEL {key}"));
                    return Task.FromResult(true);
                case "ExecuteAsync" when args.Length == 1 && args[0] is CommandFlags:
                    Sent.Add(new Wire("EXEC"));
                    return Task.FromResult(true);
                default:
                    throw new NotSupportedException($"{name} is not scripted.");
            }
        }

        public class Proxy : DispatchProxy
        {
            public FakeRedis? Owner { get; set; }

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Owner!.Handle(targetMethod!.Name, args ?? []);
        }
    }
}
