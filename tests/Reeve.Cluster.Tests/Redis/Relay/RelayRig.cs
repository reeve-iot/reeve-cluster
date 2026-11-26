using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;

namespace Unit.Redis.Relay;

internal sealed class FakeWire : IAskWire
{
    private readonly object gate = new();
    private readonly Dictionary<string, (byte[] Value, DateTime Expires)> records = [];
    private readonly Dictionary<string, List<Action<byte[]>>> subscribers = [];
    private readonly HashSet<string> connected = [];
    private readonly List<Rule> rules = [];

    private sealed class Rule(string op, Func<byte[]?, bool> when, Exception fault, bool runFirst, int times)
    {
        public string Op { get; } = op;

        public Func<byte[]?, bool> When { get; } = when;

        public Exception Fault { get; } = fault;

        public bool RunFirst { get; } = runFirst;

        public int Times { get; set; } = times;
    }

    public TimeProvider Clock { get; set; } = TimeProvider.System;

    public long ConnectionEvents { get; set; }

    private DateTime Now => Clock.GetUtcNow().UtcDateTime;

    public volatile bool Down;

    private readonly HashSet<string> hanging = [];
    private readonly List<(string Channel, int Times)> subscribeFaults = [];

    public void Hang(params string[] ops)
    {
        lock (gate)
        {
            hanging.UnionWith(ops);
        }
    }

    public void Unhang()
    {
        lock (gate)
        {
            hanging.Clear();
        }
    }

    public void FailSubscribe(string channel, int times)
    {
        lock (gate)
        {
            subscribeFaults.Add((channel, times));
        }
    }

    public int Handlers
    {
        get
        {
            lock (gate)
            {
                return subscribers.Values.Sum(l => l.Count);
            }
        }
    }

    private bool Hangs(string op)
    {
        lock (gate)
        {
            return hanging.Contains(op);
        }
    }

    private readonly List<(string Op, CancellationToken Token)> hung = [];

    private async Task<T> HangAsync<T>(string op, CancellationToken cancellationToken)
    {
        lock (gate)
        {
            hung.Add((op, cancellationToken));
        }

        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        return default!;
    }

    public bool HasLiveHang(string op)
    {
        lock (gate)
        {
            return hung.Any(h => h.Op == op && !h.Token.IsCancellationRequested);
        }
    }

    public ConcurrentQueue<string> Commands { get; } = new();

    public static Exception NotConnected() => new RedisConnectionException(ConnectionFailureType.UnableToResolvePhysicalConnection, CommandFlags.None, "down", null, CommandStatus.Unknown);

    public static Exception Lost() => new RedisConnectionException(ConnectionFailureType.SocketFailure, CommandFlags.None, "lost", null, CommandStatus.Unknown);

    public void Fail(string op, Exception fault, bool runFirst = false, int times = 1, Func<byte[]?, bool>? when = null)
    {
        lock (gate)
        {
            rules.Add(new Rule(op, when ?? (_ => true), fault, runFirst, times));
        }
    }

    public static Func<byte[]?, bool> Writing(char tag) => value => value is { Length: > 1 } && value[1] == (byte)tag;

    public byte[]? Peek(string record)
    {
        lock (gate)
        {
            return records.TryGetValue(record, out var held) && held.Expires > Now ? held.Value : null;
        }
    }

    public IEnumerable<string> Records()
    {
        lock (gate)
        {
            return records.Where(r => r.Value.Expires > Now).Select(r => r.Key).ToList();
        }
    }

    public bool HasRecordTagged(char tag) => Records().Any(r => Peek(r) is { Length: > 1 } v && v[1] == (byte)tag);

    public bool HasSubscriber(string channel)
    {
        lock (gate)
        {
            return subscribers.TryGetValue(channel, out var list) && list.Count > 0;
        }
    }

    public Task<WireRecord> ReadAsync(string record, CancellationToken cancellationToken)
    {
        if (Hangs("Read"))
        {
            return HangAsync<WireRecord>("Read", cancellationToken);
        }

        Enter("Read", null, cancellationToken, out var after);
        lock (gate)
        {
            if (records.TryGetValue(record, out var held) && held.Expires > Now)
            {
                return Done(after, new WireRecord(held.Value, held.Expires - Now));
            }
        }

        return Done(after, new WireRecord(null, null));
    }

    public Task WriteAsync(string record, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        if (Hangs("Write"))
        {
            return HangAsync<bool>("Write", cancellationToken);
        }

        Enter("Write", value, cancellationToken, out var after);
        lock (gate)
        {
            records[record] = (value, Now + timeToLive);
        }

        return DoneVoid(after);
    }

    public Task<bool> ReplaceAsync(string record, byte[] expected, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        if (Hangs("Replace"))
        {
            return HangAsync<bool>("Replace", cancellationToken);
        }

        Enter("Replace", value, cancellationToken, out var after);
        lock (gate)
        {
            if (records.TryGetValue(record, out var held) && held.Expires > Now && held.Value.AsSpan().SequenceEqual(expected))
            {
                records[record] = (value, Now + timeToLive);
                return Done(after, true);
            }
        }

        return Done(after, false);
    }

    public void Delete(string record)
    {
        lock (gate)
        {
            records.Remove(record);
        }
    }

    public Task PublishAsync(string channel, byte[] message, CancellationToken cancellationToken)
    {
        if (Hangs("Publish"))
        {
            return HangAsync<bool>("Publish", cancellationToken);
        }

        Enter("Publish", message, cancellationToken, out var after);
        Action<byte[]>[] handlers;
        lock (gate)
        {
            handlers = subscribers.TryGetValue(channel, out var list) ? [.. list] : [];
        }

        foreach (var handler in handlers)
        {
            handler(message);
        }

        return DoneVoid(after);
    }

    public Task SubscribeAsync(string channel, Action<byte[]> handler, CancellationToken cancellationToken)
    {
        lock (gate)
        {
            if (!subscribers.TryGetValue(channel, out var list))
            {
                subscribers[channel] = list = [];
            }

            list.Add(handler);
        }

        Enter("Subscribe", null, cancellationToken, out var after);
        lock (gate)
        {
            for (var i = 0; i < subscribeFaults.Count; i++)
            {
                if (subscribeFaults[i].Times > 0 && channel.Contains(subscribeFaults[i].Channel, StringComparison.Ordinal))
                {
                    subscribeFaults[i] = (subscribeFaults[i].Channel, subscribeFaults[i].Times - 1);
                    throw Lost();
                }
            }
        }

        lock (gate)
        {
            connected.Add(channel);
        }

        return DoneVoid(after);
    }

    public bool IsSubscribed(string channel)
    {
        lock (gate)
        {
            return connected.Contains(channel);
        }
    }

    public void LoseSubscriptions()
    {
        lock (gate)
        {
            connected.Clear();
        }
    }

    public void LoseChannel(string channel)
    {
        lock (gate)
        {
            connected.Remove(channel);
        }
    }

    public void RestoreSubscriptions()
    {
        lock (gate)
        {
            connected.UnionWith(subscribers.Keys);
        }
    }

    public void Unsubscribe(string channel)
    {
        lock (gate)
        {
            subscribers.Remove(channel);
            connected.Remove(channel);
        }
    }

    private void Enter(string op, byte[]? value, CancellationToken cancellationToken, out Exception? after)
    {
        after = null;
        cancellationToken.ThrowIfCancellationRequested();
        Commands.Enqueue(op);
        if (Down)
        {
            throw NotConnected();
        }

        lock (gate)
        {
            var rule = rules.FirstOrDefault(r => r.Op == op && r.Times > 0 && r.When(value));
            if (rule is null)
            {
                return;
            }

            rule.Times--;
            if (!rule.RunFirst)
            {
                throw rule.Fault;
            }

            after = rule.Fault;
        }
    }

    private static Task<T> Done<T>(Exception? after, T value) => after is null ? Task.FromResult(value) : Task.FromException<T>(after);

    private static Task DoneVoid(Exception? after) => after is null ? Task.CompletedTask : Task.FromException(after);
}

internal sealed class ReservingInbound : IAskInbound
{
    private readonly Func<AskDelivery, CancellationToken, ValueTask<AskReservation>> decide;
    private int started;
    private int reserved;

    public ReservingInbound(Func<AskDelivery, CancellationToken, ValueTask<AskReservation>> decide) => this.decide = decide;

    public int Reserved => Volatile.Read(ref reserved);

    public int Started => Volatile.Read(ref started);

    public static ReservingInbound Handling(ReservingInboundHandler handle, Action? onReserve = null)
    {
        ReservingInbound? self = null;
        self = new ReservingInbound((delivery, token) =>
        {
            onReserve?.Invoke();
            Interlocked.Increment(ref self!.reserved);
            return ValueTask.FromResult(AskReservation.Reserve(() =>
            {
                Interlocked.Increment(ref self.started);
                return handle(delivery, token);
            }));
        });
        return self;
    }

    public static ReservingInbound Echoing() =>
        Handling((delivery, _) => Task.FromResult(new AskAnswer(AskAnswerKind.Response, delivery.Payload.ToArray(), null)));

    public static ReservingInbound Declining(DeclineReason reason) =>
        new((_, _) => ValueTask.FromResult(AskReservation.Decline(reason)));

    public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy) => throw new NotSupportedException("The relay reserves.");

    public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy) => decide(delivery, answerBy);
}

internal delegate Task<AskAnswer> ReservingInboundHandler(AskDelivery delivery, CancellationToken answerBy);

internal sealed class RelayRig : IAsyncDisposable
{
    public static readonly AskRelayTimings Timings = new(
        TimeSpan.FromMilliseconds(20), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(20));

    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(1);

    public static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    public RelayRig(IAskInbound target, FakeWire? wire = null, IAskInbound? caller = null, TimeProvider? time = null, AskRelayTimings? timings = null, TimeProvider? targetTime = null)
    {
        this.time = time ?? TimeProvider.System;
        this.timings = timings ?? Timings;
        Wire = wire ?? new FakeWire();
        Wire.Clock = this.time;
        TargetIdentity = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        CallerIdentity = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        Target = Relay("b", TargetIdentity, target, targetTime);
        Caller = Relay("a", CallerIdentity, caller ?? ReservingInbound.Echoing());
    }

    private readonly TimeProvider time;
    private readonly AskRelayTimings timings;

    public FakeWire Wire { get; }

    public MemberIdentity TargetIdentity { get; }

    public MemberIdentity CallerIdentity { get; }

    public AskRelay Target { get; }

    public AskRelay Caller { get; }

    public AskTarget TargetAddress => new("b", TargetIdentity.ProcessId);

    public AskRelay Relay(string replica, MemberIdentity identity, IAskInbound inbound, TimeProvider? clock = null) =>
        new(Wire, inbound, identity, Options.Create(new ClusterOptions { ReplicaId = replica }), timings, clock ?? time, NullLogger<AskRelay>.Instance);

    public async Task StartAsync()
    {
        Target.Start();
        Caller.Start();
        await Target.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
        await Caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
    }

    public Task<DeliveryOutcome> AskTargetAsync(byte[]? payload = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
        Caller.DeliverAsync(TargetAddress, new AskDelivery(null, null, null, payload ?? [1]), timeout ?? Patience, cancellationToken);

    public async Task<DeliveryOutcome> AskUntilTheWindowEndsAsync(FakeTimeProvider clock, Func<bool>? targetSettled = null, byte[]? payload = null, TimeSpan? timeout = null)
    {
        var span = timeout ?? Timeout;
        var asking = AskTargetAsync(payload, span);
        if (targetSettled is not null)
        {
            await SettledAsync(targetSettled);
        }

        clock.Advance(AskRelayDecisions.AnswerWindow(span, timings.WithdrawBudget));
        return await asking.WaitAsync(TimeSpan.FromSeconds(10));
    }

    public static async Task<T> StepUntilAsync<T>(Task<T> call, FakeTimeProvider clock, TimeSpan step)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!call.IsCompleted)
        {
            clock.Advance(step);
            await Task.Delay(1, limit.Token);
        }

        return await call;
    }

    public static void EndWindow(FakeTimeProvider clock, TimeSpan timeout) => clock.Advance(AskRelayDecisions.AnswerWindow(timeout, Timings.WithdrawBudget));

    public static async Task SettledAsync(Func<bool> condition)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(1, limit.Token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Caller.DisposeAsync();
        await Target.DisposeAsync();
    }
}
