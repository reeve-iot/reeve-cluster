namespace Reeve.Cluster.Harness.Time;

internal sealed class DriftingTimeProvider : TimeProvider
{
    private readonly TimeProvider real;
    private readonly object gate = new();
    private readonly HashSet<DriftTimer> timers = [];
    private readonly List<DriftTimer> held = [];
    private readonly DateTimeOffset wallOrigin;
    private readonly long originVirtual;
    private Anchor anchor;
    private TimeSpan wallStep;
    private bool paused;

    public DriftingTimeProvider(TimeProvider? real = null)
    {
        this.real = real ?? System;
        var now = this.real.GetTimestamp();
        anchor = new Anchor(now, now, 1.0);
        originVirtual = now;
        wallOrigin = this.real.GetUtcNow();
    }

    public double Rate => Volatile.Read(ref anchor).Rate;

    public bool IsPaused
    {
        get
        {
            lock (gate)
            {
                return paused;
            }
        }
    }

    public int LiveTimers
    {
        get
        {
            lock (gate)
            {
                return timers.Count;
            }
        }
    }

    public override long TimestampFrequency => real.TimestampFrequency;

    public override long GetTimestamp() => VirtualAt(real.GetTimestamp());

    public override DateTimeOffset GetUtcNow()
    {
        TimeSpan step;
        lock (gate)
        {
            step = wallStep;
        }

        var elapsed = (VirtualAt(real.GetTimestamp()) - originVirtual) / (double)TimestampFrequency;
        return wallOrigin + TimeSpan.FromSeconds(elapsed) + step;
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new DriftTimer(this, callback, state);
        lock (gate)
        {
            timers.Add(timer);
        }

        timer.Change(dueTime, period);
        return timer;
    }

    public void SetRate(double rate)
    {
        if (rate <= 0 || double.IsNaN(rate) || double.IsInfinity(rate))
        {
            throw new ArgumentOutOfRangeException(nameof(rate), rate, "A clock runs forward.");
        }

        lock (gate)
        {
            var now = real.GetTimestamp();
            anchor = new Anchor(now, VirtualAt(now), rate);
            Retarget();
        }
    }

    public void Shift(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        lock (gate)
        {
            var now = real.GetTimestamp();
            anchor = new Anchor(now, VirtualAt(now) + (long)(delta.TotalSeconds * TimestampFrequency), anchor.Rate);
            Retarget();
        }
    }

    public void StepWall(TimeSpan delta)
    {
        lock (gate)
        {
            wallStep += delta;
        }
    }

    public void Pause()
    {
        lock (gate)
        {
            paused = true;
        }
    }

    public void Resume()
    {
        DriftTimer[] due;
        lock (gate)
        {
            if (!paused)
            {
                return;
            }

            paused = false;
            due = [.. held.OrderBy(t => t.DueVirtual)];
            held.Clear();
        }

        if (due.Length > 0)
        {
            ThreadPool.UnsafeQueueUserWorkItem(
                static timers =>
                {
                    foreach (var timer in timers)
                    {
                        timer.FireDue();
                    }
                },
                due,
                preferLocal: false);
        }
    }

    private long VirtualAt(long realTimestamp)
    {
        var a = Volatile.Read(ref anchor);
        return a.Virtual + (long)((realTimestamp - a.Real) * a.Rate);
    }

    private void Retarget()
    {
        foreach (var timer in timers)
        {
            timer.Arm();
        }
    }

    private sealed record Anchor(long Real, long Virtual, double Rate);

    private sealed class DriftTimer : ITimer
    {
        private readonly DriftingTimeProvider owner;
        private readonly TimerCallback callback;
        private readonly object? state;
        private readonly ITimer inner;
        private long dueVirtual = -1;
        private long periodTicks;
        private bool disposed;

        public DriftTimer(DriftingTimeProvider owner, TimerCallback callback, object? state)
        {
            this.owner = owner;
            this.callback = callback;
            this.state = state;
            inner = owner.real.CreateTimer(_ => FireDue(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        public long DueVirtual
        {
            get
            {
                lock (owner.gate)
                {
                    return dueVirtual;
                }
            }
        }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (owner.gate)
            {
                if (disposed)
                {
                    return false;
                }

                owner.held.Remove(this);
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    dueVirtual = -1;
                    periodTicks = 0;
                }
                else
                {
                    var frequency = owner.TimestampFrequency;
                    dueVirtual = owner.VirtualAt(owner.real.GetTimestamp()) + (long)(dueTime.TotalSeconds * frequency);
                    periodTicks = period == Timeout.InfiniteTimeSpan || period <= TimeSpan.Zero ? 0 : (long)(period.TotalSeconds * frequency);
                }

                Arm();
                return true;
            }
        }

        public void Arm()
        {
            if (disposed || dueVirtual < 0)
            {
                inner.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                return;
            }

            var remaining = dueVirtual - owner.VirtualAt(owner.real.GetTimestamp());
            if (remaining <= 0)
            {
                inner.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);
                return;
            }

            var seconds = remaining / (double)owner.TimestampFrequency / owner.anchor.Rate;
            var ms = Math.Min(Math.Ceiling(seconds * 1000.0), uint.MaxValue - 2);
            inner.Change(TimeSpan.FromMilliseconds(Math.Max(ms, 1)), Timeout.InfiniteTimeSpan);
        }

        public void FireDue()
        {
            lock (owner.gate)
            {
                if (disposed || dueVirtual < 0)
                {
                    return;
                }

                var now = owner.VirtualAt(owner.real.GetTimestamp());
                if (now < dueVirtual - (owner.TimestampFrequency / 2000))
                {
                    Arm();
                    return;
                }

                if (owner.paused)
                {
                    if (!owner.held.Contains(this))
                    {
                        owner.held.Add(this);
                    }

                    return;
                }

                if (periodTicks > 0)
                {
                    dueVirtual += periodTicks;
                    if (dueVirtual <= now)
                    {
                        dueVirtual = now + periodTicks;
                    }

                    Arm();
                }
                else
                {
                    dueVirtual = -1;
                }
            }

            callback(state);
        }

        public void Dispose()
        {
            lock (owner.gate)
            {
                if (disposed)
                {
                    return;
                }

                disposed = true;
                owner.timers.Remove(this);
                owner.held.Remove(this);
            }

            inner.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
