using System.Collections.Concurrent;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Reeve.Cluster;
using Reeve.Cluster.Log;

namespace Unit.Generations;

internal static class Stores
{
    public static GenerationStore Create(params string[] types) => Create(NoModelEvents.Instance, types);

    public static GenerationStore Create(IModelEventSink events, params string[] types) =>
        new(types, NoFaultPoints.Instance, events, TimeProvider.System, NullLogger.Instance);

    public static GenerationStore Create(ReconcilerGraph graph) =>
        new(graph, NoFaultPoints.Instance, NoModelEvents.Instance, TimeProvider.System, NullLogger.Instance);

    public static GenerationStore Create(IFaultPoints points, params string[] types) =>
        new(types, points, NoModelEvents.Instance, TimeProvider.System, NullLogger.Instance);

    public static GenerationStore Create(ILogger logger, params string[] types) =>
        new(types, NoFaultPoints.Instance, NoModelEvents.Instance, TimeProvider.System, logger);

    public static async Task AcceptAsync(this ApplierPass pass, string type, ReconcileResult result)
    {
        var outcome = await pass.RunAttemptAsync(type, _ => Task.FromResult(result), Never);
        Assert.Equal(AttemptOutcomeKind.Accepted, outcome.Kind);
    }

    public static async Task<IGeneration> PublishAsync(
        this GenerationStore store, long head, Guid? headToken, params (string Type, object State)[] states)
    {
        var pass = store.BeginPass(head, headToken, states.Select(s => s.Type));
        foreach (var (type, state) in states)
        {
            await pass.AcceptAsync(type, new ReconcileResult(state));
        }

        var generation = store.Publish(pass);
        Assert.NotNull(generation);
        return generation;
    }

    public static Task Never { get; } = new TaskCompletionSource().Task;
}

internal sealed class HeldPoint(FaultPoint held) : IFaultPoints
{
    private volatile bool armed;

    private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Reached => reached.Task;

    public void Arm() => armed = true;

    public void Release() => released.TrySetResult();

    public async ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        if (armed && point == held.Name)
        {
            reached.TrySetResult();
            await released.Task.WaitAsync(cancellationToken);
        }
    }
}

internal sealed class RecordingModelEvents : IModelEventSink
{
    private readonly ConcurrentQueue<ModelEvent> events = new();

    public IReadOnlyList<ModelEvent> Events => [.. events];

    public void Emit(in ModelEvent modelEvent) => events.Enqueue(modelEvent);
}

internal sealed class PinScope : IClusterScopeWriter
{
    public ClusterScopeKind Kind => ClusterScopeKind.ClientRequest;

    public bool Ended => false;

    public long FenceStepsAtBegin => 0;

    public IReadOnlyList<long> Seqs => [];

    public bool CommitRefused => false;

    public bool AcknowledgedAtCommit => false;

    public IGeneration? PinnedGeneration { get; private set; }

    public IReadOnlyCollection<DbTransaction> OpenTransactions => [];

    public IGeneration PinOrGet(IGeneration generation) => PinnedGeneration ??= generation;

    public void Pin(IGeneration generation)
    {
        if (PinnedGeneration is not null)
        {
            throw new InvalidOperationException("Already pinned.");
        }

        PinnedGeneration = generation;
    }

    public void RecordSeq(long seq) => throw new NotSupportedException();

    public void RecordRefusedCommit() => throw new NotSupportedException();

    public void OptOut() => throw new NotSupportedException();

    public void TransactionOpened(DbTransaction transaction) => throw new NotSupportedException();

    public void TransactionEnded(DbTransaction transaction) => throw new NotSupportedException();

    public void ClassifyAsExempt() => throw new NotSupportedException();
}

internal sealed class RecordingLogger : ILogger
{
    private readonly TaskCompletionSource<(string Message, Exception? Exception)> first =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<(string Message, Exception? Exception)> FirstError => first.Task;

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (logLevel >= LogLevel.Error)
        {
            first.TrySetResult((formatter(state, exception), exception));
        }
    }
}
