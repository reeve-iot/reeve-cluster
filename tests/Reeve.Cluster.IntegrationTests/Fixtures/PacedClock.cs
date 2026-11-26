using Microsoft.Extensions.Time.Testing;

namespace Int;

/// <summary>
/// A clock a test moves from one scheduled timer to the next, for code that is real on the database and fake on time. It is a
/// <see cref="FakeTimeProvider"/> that knows which of its timers are pending, so a test can wait until the code under test has
/// parked on a timer (<see cref="Created"/> has grown) and then advance exactly to its due time (<see cref="AdvanceToNext"/>),
/// however slowly the database answered in between. A test that advanced by real-time steps instead would overshoot by whatever
/// the runner stalled.
/// </summary>
internal sealed class PacedClock : TimeProvider
{
    private readonly FakeTimeProvider inner = new();
    private readonly object sync = new();
    private readonly List<Pending> timers = [];
    private int created;

    public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

    public override long GetTimestamp() => inner.GetTimestamp();

    public override long TimestampFrequency => inner.TimestampFrequency;

    public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

    /// <summary>Timers created so far (one per <c>CreateTimer</c> call, which is one per <c>Task.Delay</c>).</summary>
    public int Created
    {
        get
        {
            lock (sync)
            {
                return created;
            }
        }
    }

    /// <summary>The time from now to the earliest pending timer; <see langword="null"/> with none.</summary>
    public TimeSpan? NextDue
    {
        get
        {
            lock (sync)
            {
                var dues = timers.Where(t => t.Due is not null).Select(t => t.Due!.Value).ToList();
                return dues.Count == 0 ? null : dues.Min() - inner.GetUtcNow();
            }
        }
    }

    /// <summary>Whether a timer is pending that is due <paramref name="after"/> from now (for the code under test that parked on a known delay).</summary>
    public bool IsPending(TimeSpan after)
    {
        lock (sync)
        {
            var at = inner.GetUtcNow() + after;
            return timers.Any(t => t.Due == at);
        }
    }

    public void Advance(TimeSpan by) => inner.Advance(by);

    /// <summary>Moves the clock to the earliest pending timer (not back, and not at all when none is pending); whether it moved.</summary>
    public bool AdvanceToNext()
    {
        if (NextDue is not { } next)
        {
            return false;
        }

        inner.Advance(next > TimeSpan.Zero ? next : TimeSpan.Zero);
        return true;
    }

    /// <summary>Returns once more than <paramref name="created"/> timers have been created: the code under test has parked on a new one.</summary>
    public Task UntilCreatedAfterAsync(int created, string what) => Poll.UntilAsync(() => Created > created, what);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var pending = new Pending(this, period);
        pending.Timer = inner.CreateTimer(
            s =>
            {
                pending.Fired();
                callback(s);
            },
            state,
            dueTime,
            period);
        pending.Arm(dueTime);
        lock (sync)
        {
            timers.Add(pending);
            created++;
        }

        return pending;
    }

    private sealed class Pending(PacedClock owner, TimeSpan period) : ITimer
    {
        private TimeSpan currentPeriod = period;

        public ITimer Timer { get; set; } = null!;

        public DateTimeOffset? Due { get; private set; }

        public void Arm(TimeSpan dueTime)
        {
            lock (owner.sync)
            {
                Due = dueTime == Timeout.InfiniteTimeSpan ? null : owner.inner.GetUtcNow() + dueTime;
            }
        }

        public void Fired()
        {
            lock (owner.sync)
            {
                Due = currentPeriod == Timeout.InfiniteTimeSpan || currentPeriod == TimeSpan.Zero ? null : owner.inner.GetUtcNow() + currentPeriod;
            }
        }

        public bool Change(TimeSpan dueTime, TimeSpan newPeriod)
        {
            lock (owner.sync)
            {
                currentPeriod = newPeriod;
            }

            Arm(dueTime);
            return Timer.Change(dueTime, newPeriod);
        }

        public void Dispose()
        {
            lock (owner.sync)
            {
                Due = null;
            }

            Timer.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
