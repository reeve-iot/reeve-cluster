using Microsoft.Extensions.Time.Testing;

namespace Unit.Testing;

internal sealed class CountingTimeProvider : FakeTimeProvider
{
    private int created;

    public int TimersCreated => Volatile.Read(ref created);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = base.CreateTimer(callback, state, dueTime, period);
        Interlocked.Increment(ref created);
        return timer;
    }

    public async Task TimersAsync(int count)
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (TimersCreated < count)
        {
            await Task.Delay(1, bound.Token);
        }
    }
}
