using StackExchange.Redis;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Redis.Primitives;
using Reeve.Cluster.Slots;

namespace Unit.Redis;

internal sealed class Script<T>
{
    private readonly Queue<Func<T>> steps = new();

    public Script<T> Returns(T value)
    {
        steps.Enqueue(() => value);
        return this;
    }

    public Script<T> Throws(Exception exception)
    {
        steps.Enqueue(() => throw exception);
        return this;
    }

    public Script<T> Does(Func<T> step)
    {
        steps.Enqueue(step);
        return this;
    }

    public T Next(string what) => steps.Count == 0 ? throw new InvalidOperationException($"Unscripted call: {what}") : steps.Dequeue()();
}

internal static class RedisFailures
{
    public static Exception Connection() => new RedisConnectionException(ConnectionFailureType.UnableToConnect, CommandFlags.None, "the connection is cut", null, CommandStatus.Unknown);

    public static Exception Timeout() => new RedisTimeoutException(CommandFlags.None, "the command timed out", CommandStatus.Unknown);

    public static Exception ReadOnly() => Server(RedisErrorKind.ReadOnly, "READONLY You can't write against a read only replica.");

    public static Exception WrongType() => Server(RedisErrorKind.WrongType, "WRONGTYPE Operation against a key holding the wrong kind of value");

    public static Exception Server(RedisErrorKind kind, string reply) => new RedisServerException(kind, CommandFlags.None, reply);

    public static IEnumerable<object[]> Outages() =>
    [
        [Connection()],
        [Timeout()],
        [ReadOnly()],
        [new TimeoutException("the command timed out")],
    ];
}

internal sealed class ScriptedClaimRows : IRedisClaimRows
{
    public List<string> Calls { get; } = [];

    public List<(RedisSpace Space, string Key, string Holder, Guid Token, TimeSpan Ttl)> Grants { get; } = [];

    public List<(RedisSpace Space, string Key, Guid Token)> Releases { get; } = [];

    public List<(RedisSpace Space, string Key, Guid Token, byte[] Outcome, TimeSpan OutcomeTtl)> Completions { get; } = [];

    public List<(RedisSpace Space, string Key, Guid Token)> Executions { get; } = [];

    public Script<bool> Grant { get; } = new();

    public Script<RedisRecord> Read { get; } = new();

    public Script<bool> Release { get; } = new();

    public Script<bool> Execute { get; } = new();

    public Script<bool> Complete { get; } = new();

    public Task<bool> GrantAsync(RedisSpace space, string key, string holder, Guid token, TimeSpan ttl, CancellationToken cancellationToken)
    {
        Calls.Add("grant");
        Grants.Add((space, key, holder, token, ttl));
        return Task.FromResult(Grant.Next("grant"));
    }

    public Task<RedisRecord> ReadAsync(RedisSpace space, string key, CancellationToken cancellationToken)
    {
        Calls.Add("read");
        return Task.FromResult(Read.Next("read"));
    }

    public Task<bool> ReleaseAsync(RedisSpace space, string key, Guid token, CancellationToken cancellationToken)
    {
        Calls.Add("release");
        Releases.Add((space, key, token));
        return Task.FromResult(Release.Next("release"));
    }

    public Task<bool> ExecuteAsync(RedisSpace space, string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken)
    {
        Calls.Add("execute");
        Executions.Add((space, key, token));
        return Task.FromResult(Execute.Next("execute"));
    }

    public Task<bool> CompleteAsync(RedisSpace space, string key, Guid token, byte[] outcome, TimeSpan outcomeTtl, CancellationToken cancellationToken)
    {
        Calls.Add("complete");
        Completions.Add((space, key, token, outcome, outcomeTtl));
        return Task.FromResult(Complete.Next("complete"));
    }
}

internal sealed class ScriptedBudgetRows : IRedisBudgetRows
{
    public List<(string Key, string Id, TimeSpan Period)> Consumes { get; } = [];

    public List<(string Key, string Id, long Window)> Undos { get; } = [];

    public Script<RedisBudgetStatement> Consume { get; } = new();

    public Script<bool> Undo { get; } = new();

    public Task<RedisBudgetStatement> ConsumeAsync(string key, string id, TimeSpan period, CancellationToken cancellationToken)
    {
        Consumes.Add((key, id, period));
        return Task.FromResult(Consume.Next("consume"));
    }

    public Task UndoAsync(string key, string id, long window, CancellationToken cancellationToken)
    {
        Undos.Add((key, id, window));
        _ = Undo.Next("undo");
        return Task.CompletedTask;
    }
}

internal sealed class ScriptedCounterRows : IRedisCounterRows
{
    public List<string> Increments { get; } = [];

    public Script<long> Increment { get; } = new();

    public Task<long> IncrementAsync(string key, CancellationToken cancellationToken)
    {
        Increments.Add(key);
        return Task.FromResult(Increment.Next("increment"));
    }
}

internal sealed class ScriptedSlotRows : IRedisSlotRows
{
    public List<string> Calls { get; } = [];

    public List<(string Key, long Now, TimeSpan Ttl, Guid Token)> Grants { get; } = [];

    public List<(string Key, string Id, Guid Token, long Now, TimeSpan Ttl)> Renewals { get; } = [];

    public List<(string Key, string Id, Guid Token, TimeSpan Ttl)> Admissions { get; } = [];

    public List<(string Key, Guid Token)> Refusals { get; } = [];

    public List<(string Key, string Id, Guid? Token)> Releases { get; } = [];

    public List<Action<ITransaction>> Effects { get; } = [];

    public Script<long> Time { get; } = new();

    public Script<RedisSlotRead> Read { get; } = new();

    public Script<bool> Renew { get; } = new();

    public Script<long> Grant { get; } = new();

    public Script<bool> Admit { get; } = new();

    public Script<bool> Refuse { get; } = new();

    public Script<bool> Release { get; } = new();

    public Script<bool> Execute { get; } = new();

    public Task<long> TimeAsync(string key, CancellationToken cancellationToken)
    {
        Calls.Add("time");
        return Task.FromResult(Time.Next("time"));
    }

    public Task<RedisSlotRead> ReadAsync(string key, string id, CancellationToken cancellationToken)
    {
        Calls.Add("read");
        return Task.FromResult(Read.Next("read"));
    }

    public Task<bool> RenewAsync(string key, string id, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken)
    {
        Calls.Add("renew");
        Renewals.Add((key, id, token, now, ttl));
        return Task.FromResult(Renew.Next("renew"));
    }

    public Task<long> GrantAsync(string key, Guid token, long now, TimeSpan ttl, CancellationToken cancellationToken)
    {
        Calls.Add("grant");
        Grants.Add((key, now, ttl, token));
        return Task.FromResult(Grant.Next("grant"));
    }

    public Task AdmitAsync(string key, string id, Guid token, TimeSpan ttl, CancellationToken cancellationToken)
    {
        Calls.Add("admit");
        Admissions.Add((key, id, token, ttl));
        _ = Admit.Next("admit");
        return Task.CompletedTask;
    }

    public Task RefuseAsync(string key, Guid token, CancellationToken cancellationToken)
    {
        Calls.Add("refuse");
        Refusals.Add((key, token));
        _ = Refuse.Next("refuse");
        return Task.CompletedTask;
    }

    public Task<bool> ReleaseAsync(string key, string id, Guid? token, CancellationToken cancellationToken)
    {
        Calls.Add("release");
        Releases.Add((key, id, token));
        return Task.FromResult(Release.Next("release"));
    }

    public Task<bool> ExecuteAsync(string key, Guid token, Action<ITransaction> queue, CancellationToken cancellationToken)
    {
        Calls.Add("execute");
        Effects.Add(queue);
        return Task.FromResult(Execute.Next("execute"));
    }
}

internal sealed class ScriptedSlotGate : ISlotGate
{
    public bool Serving { get; set; } = true;

    public bool Holder { get; set; } = true;

    public int ServingChecks { get; private set; }

    public List<long> HolderChecks { get; } = [];

    public ClaimSeat? CheckServing()
    {
        ServingChecks++;
        return Serving ? new ClaimSeat("r1", 4, 100) : null;
    }

    public ClaimSeat? CheckHolder(long grantedEpoch)
    {
        HolderChecks.Add(grantedEpoch);
        return Holder ? new ClaimSeat("r1", grantedEpoch, 100) : null;
    }

    public void OnOtherTimeline(long epoch)
    {
    }
}

internal sealed class RecordingSlotObserver : ISlotObserver
{
    public List<SlotAcquireResult> Acquires { get; } = [];

    public List<SlotRenewResult> Renewals { get; } = [];

    public void Acquired(SlotAcquireResult result) => Acquires.Add(result);

    public void Renewed(SlotRenewResult result) => Renewals.Add(result);
}
