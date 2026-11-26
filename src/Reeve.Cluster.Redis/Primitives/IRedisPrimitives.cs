using StackExchange.Redis;

namespace Reeve.Cluster.Redis;

/// <summary>
/// What a fenced Redis effect answered, of a claim (<see cref="IRedisClaims.ExecuteAsync"/>) or of a slot (<see cref="IRedisSlots.ExecuteAsync"/>): committed,
/// refused, not serving, or an unknown outcome. The members say what each call makes of them.
/// </summary>
public enum RedisEffectResult
{
    /// <summary>The claim key held the token at <c>EXEC</c> and the caller's commands ran.</summary>
    Committed,

    /// <summary>
    /// Nothing ran. A claim: its key did not hold the token at <c>EXEC</c>, or changed or expired after the condition read it. A slot: the
    /// member's score had passed or the member was gone, or the slot's set changed after the watch or its score before it.
    /// </summary>
    Refused,

    /// <summary>
    /// No effect was sent. A claim: this replica was not serving. A slot: the slot holder check refused, or the token is not in this
    /// process's registry (a token a <c>Grant</c> policy answered, a released slot, a restarted process).
    /// </summary>
    NotServing,

    /// <summary>Redis did not answer: the commands may or may not have run. The caller treats it as neither a success nor a refusal.</summary>
    Unknown,
}

/// <summary>
/// Claims on Redis: the first caller wins for the time to live, and the winner's
/// effect on Redis is a transaction conditioned on the claim key still holding its token. Redis cannot see the lease, so the claim
/// is valid by its token and its time to live alone; a Redis failover that loses the key can grant twice. Only a serving replica
/// grants. The grant is one native <c>SET NX PX</c>, decided by no condition. When Redis does not decide it, the registered
/// <see cref="OnRedisUnavailable"/> does, and the use is counted: see <see cref="ClaimAsync"/>. A Redis claim never falls back to PostgreSQL.
/// </summary>
public interface IRedisClaims
{
    /// <summary>
    /// Takes the claim of <paramref name="key"/> for <paramref name="ttl"/> with a random 128-bit token drawn by the client. A key that
    /// exists answers <see cref="ClaimResult.Lost"/> with the holder its value names. Redis does not decide the call when the grant
    /// is not answered, when the read of the holder after a lost grant is not answered, and when three grants in a row were lost to a
    /// claim that had expired by the time it was read. Then the registered policy decides and the use is counted:
    /// <see cref="OnRedisUnavailable.Refuse"/> answers <see cref="ClaimResult.Unknown"/>, and <see cref="OnRedisUnavailable.Grant"/>
    /// answers <see cref="ClaimResult.Won"/> with a token no key holds, so the effects it is given are refused. A lost grant is never
    /// answered <see cref="ClaimResult.Lost"/> with a holder that was not read, except for a key that holds a value of the 0.3.0 layout
    /// (the token alone), whose holder is empty until the key expires.
    /// </summary>
    Task<ClaimResult> ClaimAsync(string key, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>Ends the claim if the key still holds <paramref name="token"/>; returns whether it did.</summary>
    /// <exception cref="CommitOutcomeUnknownException">Redis did not answer; the claim may or may not be released.</exception>
    Task<bool> ReleaseAsync(string key, Guid token, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs the commands <paramref name="effect"/> queues in one transaction conditioned on the claim key holding
    /// <paramref name="token"/>, under a <c>WATCH</c> of that key: a claim whose time to live passed, or that another token replaced,
    /// refuses it. The claim key is read first: when it does not hold <paramref name="token"/>, <paramref name="effect"/> is not called
    /// and the answer is <see cref="RedisEffectResult.Refused"/>. Queue the commands without awaiting them. The keys the commands address share the hash tag of
    /// <see cref="RedisPrimitiveKeys.ClaimHashTag"/> under Redis Cluster.
    /// </summary>
    Task<RedisEffectResult> ExecuteAsync(string key, Guid token, Action<ITransaction> effect, CancellationToken cancellationToken = default);
}

/// <summary>
/// Single-flight on Redis: one computation is started at a time; its result is stored with a time
/// to live by a transaction conditioned on the claim key holding the token, so a late computation's result is refused. The claim key
/// and the result key share a hash tag.
/// </summary>
public interface IRedisSingleFlight
{
    /// <summary>
    /// As <see cref="IClusterSingleFlight.RunAsync"/>. The grant is the claim's native <c>SET NX PX</c>, followed by a read of the result:
    /// a live result is answered <see cref="SingleFlightResult.Waited"/> and the grant is released. Redis does not decide the call when
    /// the grant, the read of the result or a poll for another caller's result is not answered, and when three grants in a row were lost
    /// to a claim that had expired, with no result, by the time it was read (waiting for a holder does not count toward the three). Then
    /// the registered policy decides and the use is counted: <see cref="OnRedisUnavailable.Refuse"/> answers
    /// <see cref="SingleFlightResult.Unknown"/> and computes nothing, and <see cref="OnRedisUnavailable.Grant"/> computes without a claim
    /// and answers <see cref="SingleFlightResult.ComputedUnknown"/>. A grant this call won and could not hand back is left to its time
    /// to live.
    /// </summary>
    /// <remarks>
    /// A failure of the result write that is not an outage (<c>WRONGTYPE</c>, a missing permission) is thrown, where
    /// <see cref="IClusterSingleFlight"/> answers <see cref="SingleFlightResult.ComputedUnknown"/> for any failure of its write.
    /// </remarks>
    Task<SingleFlightResult> RunAsync(
        string key, Func<CancellationToken, Task<byte[]>> compute, TimeSpan claimTtl, TimeSpan resultTtl, CancellationToken cancellationToken = default);
}

/// <summary>
/// A budget on Redis: a set of distinct ids per period, counted in an unconditional transaction.
/// The period is a window of Redis's clock starting at a multiple of the period since the Unix epoch.
/// </summary>
public interface IRedisBudget
{
    /// <summary>
    /// As <see cref="IClusterBudget.ConsumeAsync"/>. When Redis is unavailable, <see cref="OnRedisUnavailable.Refuse"/> answers
    /// <see cref="BudgetResult.Unknown"/> and <see cref="OnRedisUnavailable.Grant"/> answers <see cref="BudgetResult.Granted"/> with
    /// <c>Used</c> 0: nothing was counted.
    /// </summary>
    /// <remarks>
    /// The window comes from Redis's <c>TIME</c>, sent without a key. Under Redis Cluster the node that answers it may not be the node
    /// that holds the window's set, so window boundaries depend on which node answered; the limit per window still holds.
    /// </remarks>
    Task<BudgetResult> ConsumeAsync(string key, string id, int limit, TimeSpan period, CancellationToken cancellationToken = default);
}

/// <summary>A counter on Redis: one atomic <c>INCR</c>. It grants nothing, so it has no policy and no serving check.</summary>
public interface IRedisCounter
{
    /// <summary>Increments the counter; <see cref="CounterResult.Unknown"/> when Redis did not answer, after which a retry may count twice.</summary>
    Task<CounterResult> IncrementAsync(string key, CancellationToken cancellationToken = default);
}

/// <summary>
/// Held slots on Redis: at most <c>limit</c> concurrent holdings of a key, each a member of a sorted
/// set scored by its expiry on Redis's clock. The acquire's grant is an unconditional <c>MULTI</c>/<c>EXEC</c> with no script and no
/// <c>WATCH</c>; a concurrent over-limit pair may both be refused and is never both admitted. Redis cannot see the member, so a fenced
/// member's slot ends at its time to live; a Redis failover that loses writes can lose a grant. Only a serving replica acquires; a
/// renewal and an effect follow the slot holder check, and a release runs in every phase. When Redis is unavailable at the acquire, the registered
/// <see cref="OnRedisUnavailable"/> decides and the use is counted. A Redis slot never falls back to PostgreSQL.
/// </summary>
public interface IRedisSlots
{
    /// <summary>
    /// As <see cref="IClusterSlots.AcquireAsync"/>. When Redis does not answer a command of the decision, <see cref="OnRedisUnavailable.Refuse"/>
    /// answers <see cref="SlotAcquireResult.Unknown"/> and <see cref="OnRedisUnavailable.Grant"/> answers <see cref="SlotAcquireResult.Acquired"/>
    /// with a token no slot holds, whose renewals answer <see cref="SlotRenewResult.Ended"/>; the use is counted. After <see cref="SlotAcquireResult.Unknown"/> a retry with the same
    /// id answers the slot only when the grant's id key was written; otherwise it grants again and is counted against the limit until the first member ends.
    /// </summary>
    /// <exception cref="ArgumentException">The key or the id is empty or contains <c>{</c> or <c>}</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> is less than 1, or <paramref name="ttl"/> is not more than <c>SafetyMargin</c>.</exception>
    Task<SlotAcquireResult> AcquireAsync(string key, string id, int limit, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// As <see cref="IClusterSlots.RenewAsync"/>. A renewal Redis did not answer is <see cref="SlotRenewResult.Unknown"/> under either policy.
    /// </summary>
    Task<SlotRenewResult> RenewAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// As <see cref="IClusterSlots.ReleaseAsync"/>, except that it throws no <see cref="InvalidOperationException"/>: Redis has no slot-fenced transaction. With no token it removes the token the id's key
    /// names. It leaves the id's key, which then names a slot that is gone.
    /// </summary>
    /// <exception cref="CommitOutcomeUnknownException">Redis did not answer; the slot may or may not be released.</exception>
    Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs the commands <paramref name="effect"/> queues in one transaction conditioned on the slot's score being unchanged, under a
    /// <c>WATCH</c> of the slot set: any write to the set after the watch, and any change of the slot's score before it, refuses it, so <see cref="RedisEffectResult.Refused"/> may mean
    /// contention and the holder may retry while its renewals answer <see cref="SlotRenewResult.Renewed"/>. A slot that is not in this
    /// process's registry, or that the slot holder check refuses, answers <see cref="RedisEffectResult.NotServing"/>. Queue the commands
    /// without awaiting them; their keys share the hash tag of <see cref="RedisPrimitiveKeys.SlotHashTag"/>.
    /// </summary>
    Task<RedisEffectResult> ExecuteAsync(string key, string id, Guid token, Action<ITransaction> effect, CancellationToken cancellationToken = default);
}
