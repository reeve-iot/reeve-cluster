using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Status;

namespace Unit.Status;

public sealed class ClusterStatusTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly FakeStatusStore store = new();
    private readonly FakeTimeProvider clock = new();

    private ClusterStatus Status(TimeSpan? budget) =>
        new(store, Options.Create(new ClusterOptions { ActionTimeBudget = budget }), clock);

    private static ClusterStatusRead Read() => new() { ReadAt = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), Members = [] };

    [Fact]
    public async Task The_read_is_the_stores_answer_unchanged()
    {
        var expected = Read();
        store.OnRead = () => Task.FromResult(expected);

        Assert.Same(expected, await Status(TimeSpan.FromSeconds(30)).ReadAsync());
        Assert.Same(expected, await Status(null).ReadAsync());
    }

    [Fact]
    public async Task A_read_past_ActionTimeBudget_ends_with_a_TimeoutException()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.OnReadBound = async token =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return Read();
        };

        var read = Status(TimeSpan.FromSeconds(30)).ReadAsync();
        await entered.Task.WaitAsync(Wait);
        Assert.False(read.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(29));
        Assert.False(read.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(2));

        var thrown = await Assert.ThrowsAsync<TimeoutException>(() => read.WaitAsync(Wait));
        Assert.IsAssignableFrom<OperationCanceledException>(thrown.InnerException);
    }

    [Fact]
    public async Task A_cancel_by_the_caller_is_an_OperationCanceledException_not_a_timeout()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.OnReadBound = async token =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return Read();
        };
        using var caller = new CancellationTokenSource();

        var read = Status(TimeSpan.FromSeconds(30)).ReadAsync(caller.Token);
        await entered.Task.WaitAsync(Wait);
        await caller.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(Wait));
    }

    [Fact]
    public async Task With_no_ActionTimeBudget_only_the_callers_token_ends_the_read()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.OnReadBound = async token =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return Read();
        };
        using var caller = new CancellationTokenSource();

        var read = Status(null).ReadAsync(caller.Token);
        await entered.Task.WaitAsync(Wait);
        clock.Advance(TimeSpan.FromHours(1));
        Assert.False(read.IsCompleted);
        await caller.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(Wait));
    }

    [Fact]
    public async Task A_failure_of_the_store_reaches_the_caller_as_it_was_raised()
    {
        var fault = new InvalidOperationException("the database is down");
        store.OnRead = () => throw fault;

        Assert.Same(fault, await Assert.ThrowsAsync<InvalidOperationException>(() => Status(TimeSpan.FromSeconds(30)).ReadAsync()));
    }
}
