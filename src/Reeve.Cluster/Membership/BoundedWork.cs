namespace Reeve.Cluster.Membership;

internal static class BoundedWork
{
    public static Task<bool[]> RunAsync(TimeProvider time, TimeSpan bound, IReadOnlyList<Func<CancellationToken, Task>> works, Action<int>? ended = null)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(works);
        return new Run(works.Count, ended).Start(time, bound, works);
    }

    private sealed class Run
    {
        private readonly object sync = new();
        private readonly CancellationTokenSource cancel = new();
        private readonly TaskCompletionSource<bool[]> decided = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly int count;
        private readonly Action<int>? ended;
        private readonly bool[] inTime;
        private int pending;
        private int running;
        private bool expired;
        private Task cancelling = Task.CompletedTask;
        private ITimer? timer;

        public Run(int count, Action<int>? ended)
        {
            this.count = count;
            this.ended = ended;
            inTime = new bool[count];
            pending = count;
            running = count;
        }

        public Task<bool[]> Start(TimeProvider time, TimeSpan bound, IReadOnlyList<Func<CancellationToken, Task>> works)
        {
            if (count == 0)
            {
                cancel.Dispose();
                return Task.FromResult(Array.Empty<bool>());
            }

            if (bound <= TimeSpan.Zero)
            {
                Expire();
            }
            else
            {
                var armed = time.CreateTimer(static state => ((Run)state!).Expire(), this, bound, Timeout.InfiniteTimeSpan);
                lock (sync)
                {
                    timer = armed;
                }
            }

            var token = cancel.Token;
            for (var i = 0; i < works.Count; i++)
            {
                var index = i;
                var work = works[i];
                var task = Task.Run(() => work(token), CancellationToken.None);
                _ = task.ContinueWith(
                    static (ended, state) =>
                    {
                        var (run, i) = ((Run, int))state!;
                        run.Ended(ended, i);
                    },
                    (this, index),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }

            return decided.Task;
        }

        private void Expire()
        {
            lock (sync)
            {
                if (pending == 0 || expired)
                {
                    return;
                }

                expired = true;

                cancelling = cancel.CancelAsync();
                decided.TrySetResult([.. inTime]);
            }
        }

        private void Ended(Task task, int index)
        {
            _ = task.Exception;
            bool last;
            lock (sync)
            {
                if (!expired)
                {
                    inTime[index] = true;
                    if (--pending == 0)
                    {
                        decided.TrySetResult([.. inTime]);
                    }
                }

                last = --running == 0;
            }

            ended?.Invoke(index);
            if (last)
            {
                Release();
            }
        }

        private void Release()
        {
            ITimer? armed;
            Task cancelled;
            lock (sync)
            {
                armed = timer;
                cancelled = cancelling;
            }

            armed?.Dispose();
            if (cancelled.IsCompleted)
            {
                cancel.Dispose();
            }
            else
            {
                _ = cancelled.ContinueWith(static (_, state) => ((CancellationTokenSource)state!).Dispose(), cancel, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }
    }
}
