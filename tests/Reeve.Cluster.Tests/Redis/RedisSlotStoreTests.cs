using System.Globalization;
using System.Reflection;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Unit.Redis;

public sealed class RedisSlotStoreTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    private static readonly Guid Token = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private const string Held = "{p:slot:k}:held";
    private const string IdKey = "{p:slot:k}:id:i";

    private readonly FakeRedis redis = new();

    private RedisSlotStore Store() => new(redis.Database, "p:");

    private static string Member(Guid token) => token.ToString("N");

    private IEnumerable<string> Commands => redis.Sent.Select(s => s.Command);

    [Fact]
    public async Task The_time_is_asked_of_the_server_that_holds_the_keys_hash_slot_and_is_milliseconds()
    {
        redis.ServerName = "10.0.0.7:7000";
        redis.Time = (1_700_000_000, 123_456);

        var now = await Store().TimeAsync("k", CancellationToken.None);

        Assert.Equal(1_700_000_000_123L, now);
        Assert.Equal(["TIME@10.0.0.7:7000"], Commands);
        Assert.Equal([Held], redis.SelectedKeys);
    }

    [Fact]
    public async Task A_time_for_a_key_no_server_owns_is_asked_of_the_database()
    {
        redis.NoServerOwnsTheKey = true;
        redis.Time = (10, 500_000);

        Assert.Equal(10_500L, await Store().TimeAsync("k", CancellationToken.None));
        Assert.Equal(["TIME@database"], Commands);
    }

    [Fact]
    public async Task The_acquire_starts_the_time_and_the_read_of_the_id_key_together_and_sends_no_other_command()
    {
        redis.Time = (2, 0);
        redis.Strings[IdKey] = Member(Token);

        var read = await Store().ReadAsync("k", "i", CancellationToken.None);

        Assert.Equal(new RedisSlotRead(2_000, Token), read);
        Assert.Equal(["TIME@s1", "GET " + IdKey], Commands);
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(true, "not-a-token")]
    [InlineData(true, "11111111-2222-3333-4444-555555555555")]
    [InlineData(true, "{11111111222233334444555555555555}")]
    public async Task An_id_key_that_is_absent_or_holds_no_token_names_none(bool exists, string? value)
    {
        redis.Time = (2, 0);
        if (exists)
        {
            redis.Strings[IdKey] = value!;
        }

        Assert.Null((await Store().ReadAsync("k", "i", CancellationToken.None)).IdToken);
    }

    [Fact]
    public async Task A_grant_is_one_unconditional_transaction_that_prunes_adds_the_member_if_new_and_counts()
    {
        redis.Set(Held, "other", 99_000);

        var count = await Store().GrantAsync("k", Token, 1_000, Ttl, CancellationToken.None);

        Assert.Equal(
            ["MULTI", $"ZREMRANGEBYSCORE {Held} -inf 1000", $"ZADD {Held} NX 31000 {Member(Token)}", $"ZCARD {Held}", "EXEC"],
            Commands);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task A_grant_prunes_the_members_scored_at_or_below_the_time_and_counts_the_ones_it_leaves()
    {
        redis.Set(Held, "ended", 1_000);
        redis.Set(Held, "live", 1_001);

        Assert.Equal(2, await Store().GrantAsync("k", Token, 1_000, Ttl, CancellationToken.None));
        Assert.DoesNotContain("ended", redis.Members(Held));
    }

    [Fact]
    public async Task A_grant_that_Redis_did_not_execute_throws_and_is_never_a_count()
    {
        redis.ExecAnswer = false;

        await Assert.ThrowsAsync<InvalidOperationException>(() => Store().GrantAsync("k", Token, 1_000, Ttl, CancellationToken.None));
    }

    [Fact]
    public async Task A_renewal_is_one_unconditional_transaction_that_prunes_extends_only_an_existing_member_reads_it_and_extends_the_id_key()
    {
        redis.Set(Held, Member(Token), 20_000);

        var renewed = await Store().RenewAsync("k", "i", Token, 1_000, Ttl, CancellationToken.None);

        Assert.True(renewed);
        Assert.Equal(
            [
                "MULTI", $"ZREMRANGEBYSCORE {Held} -inf 1000", $"ZADD {Held} XX 31000 {Member(Token)}", $"ZSCORE {Held} {Member(Token)}",
                $"PEXPIRE {IdKey} 30000", "EXEC",
            ],
            Commands);
        Assert.Equal(31_000d, redis.Score(Held, Member(Token)));
    }

    [Fact]
    public async Task A_renewal_of_a_member_that_ended_by_the_time_adds_nothing_and_finds_no_score()
    {
        redis.Set(Held, Member(Token), 1_000);

        Assert.False(await Store().RenewAsync("k", "i", Token, 1_000, Ttl, CancellationToken.None));
        Assert.Null(redis.Score(Held, Member(Token)));
        Assert.Contains($"PEXPIRE {IdKey} 30000", Commands);
    }

    [Fact]
    public async Task A_renewal_of_a_member_that_is_not_there_adds_nothing()
    {
        Assert.False(await Store().RenewAsync("k", "i", Token, 1_000, Ttl, CancellationToken.None));
        Assert.Empty(redis.Members(Held));
    }

    [Fact]
    public async Task The_id_key_is_written_with_the_token_and_the_time_to_live_by_a_command_of_its_own()
    {
        await Store().AdmitAsync("k", "i", Token, Ttl, CancellationToken.None);

        var set = Assert.Single(redis.Sent);
        Assert.Equal("SET " + IdKey, set.Command);
        Assert.Equal(Member(Token), set.Value);
        Assert.Equal(Ttl, set.Expiry);
    }

    [Fact]
    public async Task A_refused_member_is_removed_by_its_own_ZREM()
    {
        redis.Set(Held, Member(Token), 31_000);
        redis.Set(Held, "other", 31_000);

        await Store().RefuseAsync("k", Token, CancellationToken.None);

        Assert.Equal([$"ZREM {Held} {Member(Token)}"], Commands);
        Assert.Equal(["other"], redis.Members(Held));
    }

    [Fact]
    public async Task A_release_by_token_is_one_ZREM_and_answers_whether_a_member_was_removed()
    {
        redis.Set(Held, Member(Token), 31_000);

        Assert.True(await Store().ReleaseAsync("k", "i", Token, CancellationToken.None));
        Assert.False(await Store().ReleaseAsync("k", "i", Token, CancellationToken.None));

        Assert.Equal([$"ZREM {Held} {Member(Token)}", $"ZREM {Held} {Member(Token)}"], Commands);
    }

    [Fact]
    public async Task A_release_by_id_removes_the_token_the_id_key_names_and_leaves_the_key()
    {
        redis.Strings[IdKey] = Member(Token);
        redis.Set(Held, Member(Token), 31_000);

        Assert.True(await Store().ReleaseAsync("k", "i", null, CancellationToken.None));

        Assert.Equal(["GET " + IdKey, $"ZREM {Held} {Member(Token)}"], Commands);
        Assert.True(redis.Strings.ContainsKey(IdKey));
    }

    [Fact]
    public async Task A_release_by_id_whose_key_names_no_token_removes_nothing()
    {
        Assert.False(await Store().ReleaseAsync("k", "i", null, CancellationToken.None));
        Assert.Equal(["GET " + IdKey], Commands);
    }

    [Fact]
    public async Task An_effect_reads_the_time_and_the_score_and_runs_the_commands_in_a_transaction_conditioned_on_the_score()
    {
        redis.Time = (1, 0);
        redis.Set(Held, Member(Token), 31_000);
        var queued = 0;

        Assert.True(await Store().ExecuteAsync("k", Token, _ => queued++, CancellationToken.None));

        Assert.Equal(1, queued);
        Assert.Equal(["TIME@s1", $"ZSCORE {Held} {Member(Token)}", "MULTI"], Commands.Take(3));
        Assert.StartsWith("WHEN ", Commands.ElementAt(3));
        Assert.Contains(Held, Commands.ElementAt(3));
        Assert.Contains(Member(Token), Commands.ElementAt(3));
        Assert.Equal("EXEC", Commands.Last());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_effect_under_a_member_that_is_not_there_or_has_ended_is_refused_and_queues_and_sends_nothing_more(bool ended)
    {
        redis.Time = (31, 0);
        if (ended)
        {
            redis.Set(Held, Member(Token), 31_000);
        }

        var queued = 0;
        Assert.False(await Store().ExecuteAsync("k", Token, _ => queued++, CancellationToken.None));

        Assert.Equal(0, queued);
        Assert.Equal(["TIME@s1", $"ZSCORE {Held} {Member(Token)}"], Commands);
    }

    [Fact]
    public async Task An_effect_whose_transaction_Redis_aborts_is_refused()
    {
        redis.Time = (1, 0);
        redis.Set(Held, Member(Token), 31_000);
        redis.ExecAnswer = false;

        Assert.False(await Store().ExecuteAsync("k", Token, _ => { }, CancellationToken.None));
    }

    private sealed record Wire(string Command, string? Value = null, TimeSpan? Expiry = null);

    private sealed class FakeRedis
    {
        private readonly Dictionary<string, Dictionary<string, double>> sets = [];

        public FakeRedis()
        {
            Database = Create<IDatabase>("database");
            Transaction = Create<ITransaction>("transaction");
            Multiplexer = Create<IConnectionMultiplexer>("multiplexer");
            Server = Create<IServer>("server");
        }

        public IDatabase Database { get; }

        public ITransaction Transaction { get; }

        public IConnectionMultiplexer Multiplexer { get; }

        public IServer Server { get; }

        public Dictionary<string, string> Strings { get; } = [];

        public List<Wire> Sent { get; } = [];

        public string ServerName { get; set; } = "s1";

        public bool NoServerOwnsTheKey { get; set; }

        public List<string> SelectedKeys { get; } = [];

        public (long Seconds, long Microseconds) Time { get; set; }

        public bool ExecAnswer { get; set; } = true;

        public void Set(string key, string member, double score)
        {
            if (!sets.TryGetValue(key, out var set))
            {
                sets[key] = set = [];
            }

            set[member] = score;
        }

        public double? Score(string key, string member) =>
            sets.TryGetValue(key, out var set) && set.TryGetValue(member, out var score) ? score : null;

        public List<string> Members(string key) => sets.TryGetValue(key, out var set) ? [.. set.Keys] : [];

        private T Create<T>(string role)
            where T : class
        {
            var proxy = DispatchProxy.Create<T, Proxy>();
            ((Proxy)(object)proxy).Owner = this;
            ((Proxy)(object)proxy).Role = role;
            return proxy;
        }

        private static string Num(double value) => double.IsNegativeInfinity(value) ? "-inf" : value.ToString("0.###", CultureInfo.InvariantCulture);

        private object? Handle(string role, string name, object?[] args)
        {
            switch (name)
            {
                case "get_Multiplexer":
                    return Multiplexer;
                case "GetServer" when args[0] is RedisKey selected:
                    SelectedKeys.Add(selected.ToString());
                    return NoServerOwnsTheKey ? throw new InvalidOperationException("No server owns the key.") : Server;
                case "ExecuteAsync" when args.Length == 2 && args[0] is "TIME":
                    Sent.Add(new Wire(role == "server" ? "TIME@" + ServerName : "TIME@database"));
                    return Task.FromResult(RedisResult.Create(
                        [RedisResult.Create((RedisValue)Time.Seconds, ResultType.BulkString), RedisResult.Create((RedisValue)Time.Microseconds, ResultType.BulkString)],
                        ResultType.Array));
                case "StringGetAsync" when args[0] is RedisKey key:
                    Sent.Add(new Wire($"GET {key}"));
                    return Task.FromResult((RedisValue)(Strings.TryGetValue(key!, out var one) ? one : RedisValue.Null));
                case "StringSetAsync" when args[0] is RedisKey key:
                    var expiry = args.OfType<TimeSpan>().Cast<TimeSpan?>().FirstOrDefault();
                    Sent.Add(new Wire($"SET {key}", (string?)(RedisValue)args[1]!, expiry));
                    Strings[key!] = ((RedisValue)args[1]!).ToString();
                    return Task.FromResult(true);
                case "CreateTransaction":
                    Sent.Add(new Wire("MULTI"));
                    return Transaction;
                case "AddCondition":
                    Sent.Add(new Wire("WHEN " + args[0]));
                    return null;
                case "ExecuteAsync" when args.Length == 1 && args[0] is CommandFlags:
                    Sent.Add(new Wire("EXEC"));
                    return Task.FromResult(ExecAnswer);
                case "SortedSetRemoveRangeByScoreAsync":
                    {
                        var key = ((RedisKey)args[0]!).ToString();
                        var (start, stop) = ((double)args[1]!, (double)args[2]!);
                        Sent.Add(new Wire($"ZREMRANGEBYSCORE {key} {Num(start)} {Num(stop)}"));
                        var removed = sets.TryGetValue(key, out var set) ? set.Where(m => m.Value >= start && m.Value <= stop).Select(m => m.Key).ToList() : [];
                        removed.ForEach(m => sets[key].Remove(m));
                        return Task.FromResult((long)removed.Count);
                    }

                case "SortedSetAddAsync":
                    {
                        var key = ((RedisKey)args[0]!).ToString();
                        var member = ((RedisValue)args[1]!).ToString();
                        var score = (double)args[2]!;
                        var when = args[3]!.ToString()!;
                        var present = Score(key, member) is not null;
                        var apply = when switch
                        {
                            "NotExists" => !present,
                            "Exists" => present,
                            _ => true,
                        };
                        Sent.Add(new Wire($"ZADD {key} {(when == "NotExists" ? "NX " : when == "Exists" ? "XX " : string.Empty)}{Num(score)} {member}"));
                        if (apply)
                        {
                            Set(key, member, score);
                        }

                        return Task.FromResult(apply && !present);
                    }

                case "SortedSetScoreAsync":
                    {
                        var key = ((RedisKey)args[0]!).ToString();
                        var member = ((RedisValue)args[1]!).ToString();
                        Sent.Add(new Wire($"ZSCORE {key} {member}"));
                        return Task.FromResult(Score(key, member));
                    }

                case "SortedSetLengthAsync":
                    {
                        var key = ((RedisKey)args[0]!).ToString();
                        Sent.Add(new Wire($"ZCARD {key}"));
                        return Task.FromResult((long)Members(key).Count);
                    }

                case "SortedSetRemoveAsync" when args[1] is RedisValue member:
                    {
                        var key = ((RedisKey)args[0]!).ToString();
                        Sent.Add(new Wire($"ZREM {key} {member}"));
                        return Task.FromResult(sets.TryGetValue(key, out var set) && set.Remove((string)member!));
                    }

                case "KeyExpireAsync" when args[1] is TimeSpan ttl:
                    Sent.Add(new Wire($"PEXPIRE {args[0]} {(long)ttl.TotalMilliseconds}"));
                    return Task.FromResult(true);
                default:
                    throw new NotSupportedException($"{name} is not scripted.");
            }
        }

        public class Proxy : DispatchProxy
        {
            public FakeRedis? Owner { get; set; }

            public string Role { get; set; } = string.Empty;

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Owner!.Handle(Role, targetMethod!.Name, args ?? []);
        }
    }
}
