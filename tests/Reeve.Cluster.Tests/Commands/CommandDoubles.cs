using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Owned;

namespace Unit.Commands;

internal sealed class StubOwner(string kind, string key, long token) : OwnedContext, IFencedOwner
{
    private int starts;
    private volatile bool refusing;

    public override string Kind => kind;

    public override string Key => key;

    public override long Token => token;

    public override IGeneration Configuration => throw new NotSupportedException();

    public override void EnsureOwner() => Start();

    public override Task EndAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public int Starts => Volatile.Read(ref starts);

    public void StopServing() => refusing = true;

    public FencedStart Start()
    {
        Interlocked.Increment(ref starts);
        return refusing ? throw new OwnershipLostException() : new FencedStart(kind, key, token, 1, 1000);
    }

    public void OnOtherTimeline()
    {
    }

    public IFaultPoints FaultPoints => NoFaultPoints.Instance;
}

internal sealed class FakeCommandTenures : ICommandTenures
{
    private readonly List<CommandTenure> running = [];
    private readonly Dictionary<(string, string), CancellationTokenSource> stops = [];
    private readonly Dictionary<(string, string), StubOwner> owners = [];

    public event Action? TenureStarted;

    public CommandTenure Start(string kind, string key, long token)
    {
        var stop = new CancellationTokenSource();
        var owner = new StubOwner(kind, key, token);
        var tenure = new CommandTenure(kind, key, token, owner, owner, stop.Token);
        lock (running)
        {
            running.Add(tenure);
            stops[(kind, key)] = stop;
            owners[(kind, key)] = owner;
        }

        TenureStarted?.Invoke();
        return tenure;
    }

    public StubOwner OwnerOf(string kind, string key)
    {
        lock (running)
        {
            return owners[(kind, key)];
        }
    }

    public void Stop(string kind, string key)
    {
        lock (running)
        {
            running.RemoveAll(t => t.Kind == kind && t.Key == key);
            stops[(kind, key)].Cancel();
        }
    }

    public IReadOnlyList<CommandTenure> RunningOf(IReadOnlyCollection<string> kinds)
    {
        lock (running)
        {
            return [.. running.Where(t => kinds.Contains(t.Kind))];
        }
    }
}

internal sealed class FakeCommandRows : ICommandRows
{
    private readonly object sync = new();
    private long next;

    public List<Row> Table { get; } = [];

    public Func<TakeAction?>? TakeOverride { get; set; }

    public Func<CompleteAction?>? CompleteOverride { get; set; }

    public Func<CancellationToken, Task>? InsertGate { get; set; }

    public Func<CancellationToken, Task>? TakeGate { get; set; }

    public Func<CancellationToken, Task>? CompleteGate { get; set; }

    public Action<CommandOutcome>? CompleteCheck { get; set; }

    private static readonly System.Text.UTF8Encoding StrictUtf8 = new(false, true);

    private static void RefuseUnstorable(string? text)
    {
        if (text is null)
        {
            return;
        }

        if (text.Contains('\0', StringComparison.Ordinal))
        {
            throw new Npgsql.PostgresException("invalid byte sequence for encoding \"UTF8\": 0x00", "ERROR", "ERROR", "22021");
        }

        _ = StrictUtf8.GetBytes(text);
    }

    public List<long> Takes { get; } = [];

    public List<long> Completions { get; } = [];

    public Row Add(long token = 0, string kind = "scheduler", string key = "k1", CommandState state = CommandState.Pending, string? idempotency = null)
    {
        lock (sync)
        {
            var row = new Row
            {
                Id = ++next, Kind = kind, Key = key, Idempotency = idempotency ?? $"i{next}", Payload = [(byte)next], State = state,
                TakenToken = token == 0 ? null : token, Seq = next, CreatedAt = Time.GetUtcNow(),
            };
            Table.Add(row);
            return row;
        }
    }

    public async Task<SentCommand?> InsertAsync(string kind, string key, string idempotencyKey, byte[] payload, CancellationToken cancellationToken)
    {
        if (InsertGate is { } gate)
        {
            await gate(cancellationToken).ConfigureAwait(false);
        }

        lock (sync)
        {
            if (Table.Any(r => (r.Kind, r.Key, r.Idempotency) == (kind, key, idempotencyKey)))
            {
                return null;
            }

            var row = Add(kind: kind, key: key, idempotency: idempotencyKey);
            row.Payload = payload;
            return new SentCommand(row.Id, CommandState.Pending, payload);
        }
    }

    public Task<SentCommand?> ReadAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            var row = Table.FirstOrDefault(r => (r.Kind, r.Key, r.Idempotency) == (kind, key, idempotencyKey));
            return Task.FromResult(row is null ? null : new SentCommand(row.Id, row.State, row.Payload));
        }
    }

    public Task<bool> ResendAsync(long id, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            var row = Table.FirstOrDefault(r => r.Id == id);
            var step = row is null ? null : CommandStateMachine.Decide(CommandWriter.Resend, new CommandRow(row.State, row.TakenToken));
            if (row is null || step is not { } resent)
            {
                return Task.FromResult(false);
            }

            row.State = resent.To!.Value;
            row.TakenToken = resent.TakenToken;
            row.CreatedAt = Time.GetUtcNow();
            row.CompletedAt = null;
            return Task.FromResult(true);
        }
    }

    public Task<IReadOnlyList<(string Kind, string Key)>> ListActiveKeysAsync(IReadOnlyCollection<string> kinds, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            IReadOnlyList<(string, string)> keys = [.. Table.Where(r => kinds.Contains(r.Kind) && r.State is CommandState.Pending or CommandState.Taken).Select(r => (r.Kind, r.Key)).Distinct()];
            return Task.FromResult(keys);
        }
    }

    public Action? AfterRead { get; set; }

    public Task<IReadOnlyList<ActiveCommandRow>> ReadActiveAsync(string kind, string key, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            var mine = Table.Where(r => r.Kind == kind && r.Key == key).OrderBy(r => r.Seq).ToList();
            IReadOnlyList<ActiveCommandRow> rows = mine.Where(r => r.State == CommandState.Taken)
                .Concat(mine.Where(r => r.State == CommandState.Pending).Take(1))
                .OrderBy(r => r.Seq).Select(r => r.Active()).ToList();
            var hook = AfterRead;
            AfterRead = null;
            hook?.Invoke();
            return Task.FromResult(rows);
        }
    }

    public async Task<TakeResult> TakeAsync(IFencedOwner owner, long id, CancellationToken cancellationToken)
    {
        var start = owner.Start();
        if (TakeGate is { } gate)
        {
            await gate(cancellationToken).ConfigureAwait(false);
        }

        lock (sync)
        {
            var row = Table.First(r => r.Id == id);
            Takes.Add(id);
            var step = CommandStateMachine.Decide(CommandWriter.Take, new CommandRow(row.State, row.TakenToken), start.Token);
            if (step is not { } taken || Table.Any(t => t.Kind == row.Kind && t.Key == row.Key && t.State == CommandState.Taken))
            {
                return new TakeResult(TakeAction.NotTaken, null);
            }

            row.State = taken.To!.Value;
            row.TakenToken = taken.TakenToken;
            return new TakeResult(TakeOverride?.Invoke() ?? TakeAction.Run, row.Active());
        }
    }

    public async Task<CompleteAction> CompleteAsync(IFencedOwner owner, long id, CommandOutcome outcome, CancellationToken cancellationToken)
    {
        var start = owner.Start();
        if (CompleteGate is { } gate)
        {
            await gate(cancellationToken).ConfigureAwait(false);
        }

        RefuseUnstorable(outcome.Error);
        CompleteCheck?.Invoke(outcome);

        lock (sync)
        {
            Completions.Add(id);
            var action = CompleteOverride?.Invoke();
            var row = Table.First(r => r.Id == id);
            var step = CommandStateMachine.Decide(CommandWriter.Complete, new CommandRow(row.State, row.TakenToken), start.Token);
            if (step is not { } done)
            {
                return CompleteAction.Refused;
            }

            row.State = done.To!.Value;
            row.TakenToken = done.TakenToken;
            row.Result = outcome.Error is null ? outcome.Result.ToArray() : null;
            row.Error = outcome.Error;
            row.CompletedAt = Time.GetUtcNow();
            return action ?? CompleteAction.Completed;
        }
    }

    public Func<TakeAction?>? RetakeOverride { get; set; }

    public Func<MarkAction?>? MarkOverride { get; set; }

    public List<long> Retakes { get; } = [];

    public List<long> Marks { get; } = [];

    public async Task<TakeResult> RetakeAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken)
    {
        var start = owner.Start();
        if (TakeGate is { } gate)
        {
            await gate(cancellationToken).ConfigureAwait(false);
        }

        lock (sync)
        {
            var row = Table.First(r => r.Id == id);
            Retakes.Add(id);
            var step = row.TakenToken == oldToken
                ? CommandStateMachine.Decide(CommandWriter.Retake, new CommandRow(row.State, row.TakenToken), start.Token)
                : null;
            if (step is not { } retaken)
            {
                return new TakeResult(TakeAction.NotTaken, null);
            }

            row.State = retaken.To!.Value;
            row.TakenToken = retaken.TakenToken;
            return new TakeResult(RetakeOverride?.Invoke() ?? TakeAction.Run, row.Active());
        }
    }

    public async Task<MarkAction> MarkInDoubtAsync(IFencedOwner owner, long id, long oldToken, CancellationToken cancellationToken)
    {
        var start = owner.Start();
        if (CompleteGate is { } gate)
        {
            await gate(cancellationToken).ConfigureAwait(false);
        }

        lock (sync)
        {
            var row = Table.First(r => r.Id == id);
            Marks.Add(id);
            var step = row.TakenToken == oldToken
                ? CommandStateMachine.Decide(CommandWriter.MarkInDoubt, new CommandRow(row.State, row.TakenToken), start.Token)
                : null;
            if (step is not { } marked)
            {
                return MarkAction.Refused;
            }

            row.State = marked.To!.Value;
            row.TakenToken = marked.TakenToken;
            row.CompletedAt = Time.GetUtcNow();
            return MarkOverride?.Invoke() ?? MarkAction.Marked;
        }
    }

    public FakeTimeProvider Time { get; } = new();

    public TimeSpan PendingTtl { get; set; } = TimeSpan.FromMinutes(10);

    public TimeSpan Retention { get; set; } = TimeSpan.FromHours(24);

    public HashSet<(string Kind, string Key)> ValidHolds { get; } = [];

    public Action<long>? BeforeExpire { get; set; }

    public List<long> Expired { get; } = [];

    public Task<IReadOnlyList<ExpirableCommand>> ListExpirableAsync(int limit, CancellationToken cancellationToken)
    {
        lock (sync)
        {
            IReadOnlyList<ExpirableCommand> due = [.. Table
                .Where(r => Due(r))
                .OrderBy(r => r.CreatedAt).ThenBy(r => r.Id).Take(limit).Select(r => new ExpirableCommand(r.Id, r.Kind, r.Key))];
            return Task.FromResult(due);
        }
    }

    private bool Due(Row row) =>
        row.State == CommandState.Pending && Time.GetUtcNow() - row.CreatedAt > PendingTtl && !ValidHolds.Contains((row.Kind, row.Key));

    public Exception? ExpireFailure { get; set; }

    public Task<bool> ExpireAsync(long id, CancellationToken cancellationToken)
    {
        BeforeExpire?.Invoke(id);
        if (ExpireFailure is { } failure)
        {
            throw failure;
        }

        lock (sync)
        {
            var row = Table.FirstOrDefault(r => r.Id == id);
            if (row is null || !Due(row)
                || CommandStateMachine.Decide(CommandWriter.Expire, new CommandRow(row.State, row.TakenToken)) is not { } step)
            {
                return Task.FromResult(false);
            }

            row.State = step.To!.Value;
            row.CompletedAt = Time.GetUtcNow();
            Expired.Add(id);
            return Task.FromResult(true);
        }
    }

    public Exception? RetentionFailure { get; set; }

    public Task<int> DeleteRetainedAsync(int limit, CancellationToken cancellationToken)
    {
        if (RetentionFailure is { } failure)
        {
            throw failure;
        }

        lock (sync)
        {
            var gone = Table
                .Where(r => CommandStateMachine.Decide(CommandWriter.Retention, new CommandRow(r.State, r.TakenToken)) is not null
                    && r.CompletedAt is { } at && Time.GetUtcNow() - at > Retention)
                .Take(limit).ToList();
            Table.RemoveAll(gone.Contains);
            return Task.FromResult(gone.Count);
        }
    }

    public int StatusReads => Volatile.Read(ref statusReads);

    private int statusReads;

    public Func<Exception?>? StatusFailure { get; set; }

    public Task<CommandStatus?> ReadStatusAsync(string kind, string key, string idempotencyKey, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref statusReads);
        if (StatusFailure?.Invoke() is { } failure)
        {
            throw failure;
        }

        lock (sync)
        {
            var row = Table.FirstOrDefault(r => (r.Kind, r.Key, r.Idempotency) == (kind, key, idempotencyKey));
            return Task.FromResult(row is null ? null : (CommandStatus?)new CommandStatus(row.Id, row.State, row.Result, row.Error));
        }
    }

    public sealed class Row
    {
        public long Id { get; set; }

        public long Seq { get; set; }

        public string Kind { get; set; } = string.Empty;

        public string Key { get; set; } = string.Empty;

        public string Idempotency { get; set; } = string.Empty;

        public byte[] Payload { get; set; } = [];

        public CommandState State { get; set; }

        public long? TakenToken { get; set; }

        public byte[]? Result { get; set; }

        public string? Error { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public DateTimeOffset? CompletedAt { get; set; }

        public ActiveCommandRow Active() => new(Id, State, TakenToken, Idempotency, Payload);
    }
}

internal sealed class ScriptedCommandHandler(Func<DurableCommand, CancellationToken, Task<ReadOnlyMemory<byte>>>? run = null) : IDurableCommandHandler
{
    private readonly List<long> ran = [];
    private int running;
    private int maxRunning;

    public IReadOnlyList<long> Ran
    {
        get
        {
            lock (ran)
            {
                return [.. ran];
            }
        }
    }

    public int MaxConcurrent => Volatile.Read(ref maxRunning);

    public async Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken)
    {
        var now = Interlocked.Increment(ref running);
        InterlockedMax(ref maxRunning, now);
        lock (ran)
        {
            ran.Add(command.Id);
        }

        try
        {
            return run is null ? new byte[] { (byte)command.Id } : await run(command, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref running);
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        do
        {
            seen = Volatile.Read(ref target);
        }
        while (value > seen && Interlocked.CompareExchange(ref target, value, seen) != seen);
    }
}

internal sealed class CommandLifetime : Microsoft.Extensions.Hosting.IHostApplicationLifetime, IDisposable
{
    private readonly CancellationTokenSource stopping = new();

    public CancellationToken ApplicationStarted => CancellationToken.None;

    public CancellationToken ApplicationStopping => stopping.Token;

    public CancellationToken ApplicationStopped => CancellationToken.None;

    public void StopApplication() => stopping.Cancel();

    public void Dispose() => stopping.Dispose();
}
