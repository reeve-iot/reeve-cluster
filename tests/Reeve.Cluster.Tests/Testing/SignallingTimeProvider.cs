using Microsoft.Extensions.Time.Testing;

namespace Unit.Testing;

internal sealed class SignallingTimeProvider : FakeTimeProvider
{
    private readonly object sync = new();
    private readonly Dictionary<TimeSpan, int> created = [];
    private readonly Dictionary<TimeSpan, TaskCompletionSource> signals = [];
    private readonly List<(TimeSpan? Due, TaskCompletionSource<DateTimeOffset> Armed)> delayWatches = [];

    public bool ThrowOnTimestamp { get; set; }

    public override long GetTimestamp() => ThrowOnTimestamp ? throw new InvalidOperationException("clock failed") : base.GetTimestamp();

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = base.CreateTimer(callback, state, dueTime, period);
        var dueAt = GetUtcNow() + dueTime;
        TaskCompletionSource? signal;
        List<TaskCompletionSource<DateTimeOffset>> armed = [];
        lock (sync)
        {
            created[dueTime] = created.GetValueOrDefault(dueTime) + 1;
            signals.Remove(dueTime, out signal);
            if (state is Task)
            {
                foreach (var watch in delayWatches.Where(w => w.Due is null || w.Due == dueTime).ToList())
                {
                    delayWatches.Remove(watch);
                    armed.Add(watch.Armed);
                }
            }
        }

        signal?.TrySetResult();
        foreach (var watch in armed)
        {
            watch.TrySetResult(dueAt);
        }

        return timer;
    }

    public async Task TimerAsync(TimeSpan dueTime)
    {
        while (true)
        {
            Task wait;
            lock (sync)
            {
                if (created.GetValueOrDefault(dueTime) > 0)
                {
                    created[dueTime]--;
                    return;
                }

                if (!signals.TryGetValue(dueTime, out var signal))
                {
                    signal = NewSignal();
                    signals[dueTime] = signal;
                }

                wait = signal.Task;
            }

            await wait.WaitAsync(Bounds.Safety);
        }
    }

    public Task<DateTimeOffset> DelayArmed(TimeSpan? dueTime = null)
    {
        var armed = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (sync)
        {
            delayWatches.Add((dueTime, armed));
        }

        return armed.Task;
    }

    public async Task<(T Result, TimeSpan Moved)> RunDelaysAsync<T>(Func<Task<T>> start, TimeSpan limit)
    {
        var began = GetUtcNow();
        var next = DelayArmed();
        var work = start();
        while (await Task.WhenAny(work, next).WaitAsync(Bounds.Safety) != work)
        {
            var dueAt = await next;
            if (dueAt - began > limit)
            {
                throw new InvalidOperationException($"The work had not ended after {limit} of fake time.");
            }

            next = DelayArmed();
            if (dueAt > GetUtcNow())
            {
                SetUtcNow(dueAt);
            }
        }

        return (await work, GetUtcNow() - began);
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
