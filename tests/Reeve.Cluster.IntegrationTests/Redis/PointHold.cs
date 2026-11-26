using Reeve.Cluster;

namespace Int.Redis;

internal sealed class PointHold(string target) : IFaultPoints
{
    private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int armed;

    public Task Reached => reached.Task;

    public void Arm() => Volatile.Write(ref armed, 1);

    public void Release() => released.TrySetResult();

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        if (point != target || Interlocked.Exchange(ref armed, 0) == 0)
        {
            return ValueTask.CompletedTask;
        }

        reached.TrySetResult();
        return new ValueTask(released.Task.WaitAsync(cancellationToken));
    }
}
