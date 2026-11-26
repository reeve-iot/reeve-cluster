using System.Collections.Concurrent;

namespace Reeve.Cluster.AspNetCore;

internal sealed class RequestTracker
{
    private readonly ConcurrentDictionary<long, TaskCompletionSource> running = new();
    private long last;
    private long mark = -1;

    public RequestTracker() => HttpDiagnostics.TrackRequests(this);

    internal static readonly object TicketKey = new();

    public IDisposable Enter()
    {
        var ticket = Interlocked.Increment(ref last);
        var ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        running[ticket] = ended;
        return new Ticket(this, ticket, ended);
    }

    public int Running => running.Count;

    public int CloseAtDrain()
    {
        Interlocked.Exchange(ref mark, Interlocked.Read(ref last));
        return running.Count(r => r.Key <= Interlocked.Read(ref mark));
    }

    internal int AwaitedAtDrain
    {
        get
        {
            var at = Interlocked.Read(ref mark);
            return at < 0 ? -1 : running.Count(r => r.Key <= at);
        }
    }

    public IReadOnlyList<Task> RunningAtMark()
    {
        var at = Interlocked.Read(ref mark);
        return [.. running.Where(r => r.Key <= at).Select(r => r.Value.Task)];
    }

    private sealed class Ticket(RequestTracker tracker, long ticket, TaskCompletionSource ended) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                tracker.running.TryRemove(ticket, out _);
                ended.TrySetResult();
            }
        }
    }
}
