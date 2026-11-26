using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Membership;
using Unit.Testing;
using static Unit.Ask.ScriptedTransport;

namespace Unit.Ask;

public sealed class AskDeadlineTests
{
    private const int Runs = 50;
    private static readonly byte[] Request = [9];

    private sealed class Counting
    {
        private int calls;
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Calls => Volatile.Read(ref calls);

        public Task Started => started.Task;

        public LambdaHandler Handler(bool honoursToken) => new(async (_, ct) =>
        {
            Interlocked.Increment(ref calls);
            started.TrySetResult();
            await (honoursToken ? Task.Delay(Timeout.Infinite, ct) : new TaskCompletionSource().Task);
            return default;
        });
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task A_handler_still_running_when_the_wait_ends_is_TakenUnanswered_on_the_fake_clock(bool keyed, bool honoursToken)
    {
        var clock = new FakeTimeProvider();
        var outcomes = new List<(AskOutcome Outcome, string? Error)>();
        for (var run = 0; run < Runs; run++)
        {
            var counting = new Counting();
            var handler = counting.Handler(honoursToken);
            using var rig = new AskRig(clock, keyed ? null : handler, keyed ? handler : null);
            Setup(rig, keyed);

            var ask = Ask(rig, keyed, TimeSpan.FromSeconds(1));
            await counting.Started.WaitAsync(TimeSpan.FromSeconds(30));
            clock.Advance(TimeSpan.FromSeconds(1) + TimeSpan.FromMilliseconds(1));

            var result = await ask.WaitAsync(TimeSpan.FromSeconds(30));
            outcomes.Add((result.Outcome, result.Error));
        }

        Assert.All(outcomes, o => Assert.Equal((AskOutcome.TakenUnanswered, (string?)null), o));
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task A_handler_still_running_when_the_wait_ends_is_TakenUnanswered_on_the_system_clock(bool keyed, bool honoursToken)
    {
        var counting = new Counting();
        var handler = counting.Handler(honoursToken);
        using var rig = new AskRig(TimeProvider.System, keyed ? null : handler, keyed ? handler : null);
        Setup(rig, keyed);

        var results = await Task.WhenAll(Enumerable.Range(0, Runs).Select(_ => Ask(rig, keyed, TimeSpan.FromMilliseconds(200))));

        Assert.All(results, r => Assert.Equal((AskOutcome.TakenUnanswered, (string?)null), (r.Outcome, r.Error)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_caller_that_cancels_while_the_handler_runs_gets_TakenUnanswered(bool keyed)
    {
        for (var run = 0; run < Runs; run++)
        {
            var counting = new Counting();
            var handler = counting.Handler(honoursToken: true);
            using var rig = new AskRig(TimeProvider.System, keyed ? null : handler, keyed ? handler : null);
            Setup(rig, keyed);
            using var cancel = new CancellationTokenSource();

            var ask = Ask(rig, keyed, TimeSpan.FromMinutes(1), cancel.Token);
            await counting.Started.WaitAsync(TimeSpan.FromSeconds(30));
            await cancel.CancelAsync();

            var result = await ask.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal((AskOutcome.TakenUnanswered, (string?)null), (result.Outcome, result.Error));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task No_time_left_after_resolution_is_NotTaken_with_no_handler_call(bool keyed)
    {
        var clock = new CountingTimeProvider();
        var counting = new Counting();
        var handler = counting.Handler(honoursToken: true);
        using var rig = new AskRig(clock, keyed ? null : handler, keyed ? handler : null);
        Setup(rig, keyed);
        rig.Directory.OnResolve = () => clock.Advance(TimeSpan.FromSeconds(1) + TimeSpan.FromMilliseconds(5));

        var result = await Ask(rig, keyed, TimeSpan.FromSeconds(1));

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Equal(0, counting.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_caller_gone_after_resolution_is_NotTaken_with_no_handler_call(bool keyed)
    {
        var counting = new Counting();
        var handler = counting.Handler(honoursToken: true);
        using var rig = new AskRig(TimeProvider.System, keyed ? null : handler, keyed ? handler : null);
        Setup(rig, keyed);
        using var cancel = new CancellationTokenSource();
        rig.Directory.OnResolve = () => cancel.Cancel();

        var result = await Ask(rig, keyed, TimeSpan.FromSeconds(30), cancel.Token);

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Equal(0, counting.Calls);
    }

    private sealed class SkewedClock : TimeProvider
    {
        private long skew;

        public override long GetTimestamp() => base.GetTimestamp() + Interlocked.Read(ref skew);

        public void Skip(TimeSpan by) => Interlocked.Add(ref skew, (long)(by.TotalSeconds * TimestampFrequency));
    }

    [Theory]
    [InlineData("keyed", true)]
    [InlineData("keyed", false)]
    [InlineData("member", true)]
    [InlineData("member", false)]
    public async Task No_time_left_by_the_clock_alone_is_NotTaken_and_nothing_is_delivered(string target, bool remote)
    {
        var clock = new SkewedClock();
        var counting = new Counting();
        var handler = counting.Handler(honoursToken: true);
        var transport = new ScriptedTransport().Then(Answers(1));
        using var rig = new AskRig(clock, handler, handler, transport: transport);
        var timeout = TimeSpan.FromSeconds(30);
        rig.Directory.OnResolve = () => clock.Skip(timeout + TimeSpan.FromSeconds(1));
        if (target == "keyed")
        {
            rig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
            rig.Directory.Otherwise = remote ? new AskOwner("b", Guid.NewGuid(), 7) : rig.Mine(7);
        }
        else
        {
            rig.Directory.Member(remote ? "b" : "a", remote ? new AskMemberRow(Guid.NewGuid(), MemberRowState.Active) : rig.MyRow);
        }

        var result = target == "keyed"
            ? await rig.Ask.AskAsync(AskRig.Kind, AskRig.Key, Request, timeout)
            : await rig.Ask.AskMemberAsync(remote ? "b" : "a", Request, timeout);

        Assert.Equal(AskOutcome.NotTaken, result.Outcome);
        Assert.Empty(transport.Deliveries);
        Assert.Equal(0, counting.Calls);
    }

    [Fact]
    public async Task The_taking_side_does_not_take_a_request_whose_wait_has_ended()
    {
        var counting = new Counting();
        var handler = counting.Handler(honoursToken: true);
        using var rig = new AskRig(TimeProvider.System, handler, handler);
        rig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
        using var gone = new CancellationTokenSource();
        await gone.CancelAsync();

        var member = await rig.Inbound.TakeAsync(new AskDelivery(null, null, null, Request), gone.Token);
        var keyed = await rig.Inbound.TakeAsync(new AskDelivery(AskRig.Kind, AskRig.Key, 7, Request), gone.Token);

        Assert.False(member.IsTaken);
        Assert.False(keyed.IsTaken);
        Assert.Equal(DeclineReason.WaitEnded, member.Reason);
        Assert.Equal(DeclineReason.WaitEnded, keyed.Reason);
        Assert.Equal(0, counting.Calls);
    }

    [Fact]
    public async Task A_missing_handler_is_logged_once_per_kind()
    {
        var log = new CountingLog();
        using var rig = new AskRig(TimeProvider.System, null, null, inboundLog: log);
        rig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
        using var never = new CancellationTokenSource();

        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(DeclineReason.NoHandler, (await rig.Inbound.TakeAsync(new AskDelivery(AskRig.Kind, AskRig.Key, 7, Request), never.Token)).Reason);
            Assert.Equal(DeclineReason.NoHandler, (await rig.Inbound.TakeAsync(new AskDelivery(null, null, null, Request), never.Token)).Reason);
        }

        Assert.Equal(2, log.Warnings);
    }

    private sealed class CountingLog : ILogger<AskInbound>
    {
        private int warnings;

        public int Warnings => Volatile.Read(ref warnings);

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Interlocked.Increment(ref warnings);
            }
        }
    }

    private static void Setup(AskRig rig, bool keyed)
    {
        if (keyed)
        {
            rig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
            rig.Directory.Otherwise = rig.Mine(7);
        }
        else
        {
            rig.Directory.Member("a", rig.MyRow);
        }
    }

    private static Task<AskResult> Ask(AskRig rig, bool keyed, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        keyed
            ? rig.Ask.AskAsync(AskRig.Kind, AskRig.Key, Request, timeout, cancellationToken)
            : rig.Ask.AskMemberAsync("a", Request, timeout, cancellationToken);
}
