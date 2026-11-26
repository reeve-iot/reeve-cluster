using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class BoundedWorkTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private static Task OffContext(Func<Task> test) => Task.Run(test);

    [Fact]
    public Task Work_that_ends_before_the_bound_is_in_time_and_its_token_is_never_cancelled() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var token = CancellationToken.None;

        var inTime = await BoundedWork.RunAsync(time, S(2), [ct =>
        {
            token = ct;
            return Task.CompletedTask;
        }]).WaitAsync(Wait);

        Assert.Equal(new[] { true }, inTime);
        time.Advance(S(3));
        Assert.False(token.IsCancellationRequested);
    });

    [Fact]
    public Task At_the_bound_the_token_is_cancelled_before_the_wait_ends_and_the_work_is_not_awaited() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var work = new HangingWork();
        var run = BoundedWork.RunAsync(time, S(2), [work.RunAsync]);
        await work.Started.WaitAsync(Wait);

        time.Advance(S(2) - TimeSpan.FromTicks(1));
        Assert.False(run.IsCompleted);

        time.Advance(TimeSpan.FromTicks(1));
        Assert.Equal(new[] { false }, await run.WaitAsync(Wait));
        Assert.True(work.Token.IsCancellationRequested);
        await work.Cancelled.WaitAsync(Wait);
        work.Release();
    });

    [Fact]
    public Task I33_a_work_that_ends_as_its_cancel_fires_is_not_in_time() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var run = BoundedWork.RunAsync(time, S(2), [ct =>
        {
            var answer = new TaskCompletionSource();
            ct.Register(() => answer.TrySetResult());
            return answer.Task;
        }]);

        time.Advance(S(2));

        Assert.Equal(new[] { false }, await run.WaitAsync(Wait));
    });

    [Fact]
    public Task Each_work_has_its_own_outcome_under_one_bound() => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var hanging = new HangingWork();

        var first = new TaskCompletionSource();
        var firstEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var run = BoundedWork.RunAsync(time, S(2), [_ => first.Task, hanging.RunAsync], index =>
        {
            if (index == 0)
            {
                firstEnded.TrySetResult();
            }
        });
        await hanging.Started.WaitAsync(Wait);
        first.SetResult();
        await firstEnded.Task.WaitAsync(Wait);

        time.Advance(S(2));

        Assert.Equal(new[] { true, false }, await run.WaitAsync(Wait));
        hanging.Release();
    });

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public Task A_bound_already_passed_cancels_the_token_before_the_work_starts(double seconds) => OffContext(async () =>
    {
        var time = new FakeTimeProvider();
        var cancelledAtStart = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var inTime = await BoundedWork.RunAsync(time, S(seconds), [ct =>
        {
            cancelledAtStart.TrySetResult(ct.IsCancellationRequested);
            return Task.CompletedTask;
        }]).WaitAsync(Wait);

        Assert.Equal(new[] { false }, inTime);
        Assert.True(await cancelledAtStart.Task.WaitAsync(Wait));
    });

    [Fact]
    public Task No_work_ends_the_wait_at_once() => OffContext(async () =>
    {
        Assert.Empty(await BoundedWork.RunAsync(new FakeTimeProvider(), S(2), []).WaitAsync(Wait));
    });

    [Fact]
    public Task A_work_that_throws_is_in_time_when_it_throws_before_the_bound() => OffContext(async () =>
    {
        var inTime = await BoundedWork.RunAsync(new FakeTimeProvider(), S(2), [_ => throw new InvalidOperationException("boom")]).WaitAsync(Wait);

        Assert.Equal(new[] { true }, inTime);
    });

    [Fact]
    public Task On_the_system_clock_an_overrunning_works_token_is_cancelled_every_time() => OffContext(async () =>
    {
        for (var run = 0; run < 50; run++)
        {
            var work = new HangingWork();

            Assert.Equal(new[] { false }, await BoundedWork.RunAsync(TimeProvider.System, TimeSpan.FromMilliseconds(10), [work.RunAsync]).WaitAsync(Wait));

            await work.Cancelled.WaitAsync(Wait);
            work.Release();
        }
    });

    private sealed class HangingWork
    {
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationToken Token { get; private set; }

        public Task Started => started.Task;

        public Task Cancelled => cancelled.Task;

        public void Release() => release.TrySetResult();

        public Task RunAsync(CancellationToken cancellationToken)
        {
            Token = cancellationToken;
            cancellationToken.Register(() => cancelled.TrySetResult());
            started.TrySetResult();
            return release.Task;
        }
    }
}
