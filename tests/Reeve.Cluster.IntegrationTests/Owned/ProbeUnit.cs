using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Reeve.Cluster;

namespace Int.Owned;

internal sealed class ProbeUnit : IOwnedUnit
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> hung = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> returned = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Stopwatch>> lostStarted = new();
    private readonly ConcurrentQueue<string> lost = new();

    public Channel<(string Kind, string Key, long Token, CancellationToken Stop)> Acquired { get; } =
        Channel.CreateUnbounded<(string, string, long, CancellationToken)>();

    public IReadOnlyList<string> LostKeys => [.. lost];

    public void HangLost(string key) => hung[key] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public void ReleaseLost(string key)
    {
        if (hung.TryRemove(key, out var gate))
        {
            gate.TrySetResult();
        }
    }

    public void ReleaseAll()
    {
        foreach (var key in hung.Keys)
        {
            ReleaseLost(key);
        }
    }

    public Task LostReturned(string key) => Of(returned, key).Task;

    public Task<Stopwatch> LostStarted(string key) => Of(lostStarted, key).Task;

    public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop)
    {
        Acquired.Writer.TryWrite((context.Kind, context.Key, TokenOf(context), stop));
        return Task.Delay(Timeout.Infinite, stop);
    }

    public async Task OnLostAsync(OwnedContext context)
    {
        Of(lostStarted, context.Key).TrySetResult(Stopwatch.StartNew());
        lost.Enqueue(context.Key);
        if (hung.TryGetValue(context.Key, out var gate))
        {
            await gate.Task;
        }

        Of(returned, context.Key).TrySetResult();
    }

    private static long TokenOf(OwnedContext context)
    {
        try
        {
            return context.Token;
        }
        catch (InvalidOperationException e) when (e.Message.Contains("convergent", StringComparison.Ordinal))
        {
            return 0;
        }
    }

    private static TaskCompletionSource<T> Of<T>(ConcurrentDictionary<string, TaskCompletionSource<T>> map, string key) =>
        map.GetOrAdd(key, _ => new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously));

    private static TaskCompletionSource Of(ConcurrentDictionary<string, TaskCompletionSource> map, string key) =>
        map.GetOrAdd(key, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}
