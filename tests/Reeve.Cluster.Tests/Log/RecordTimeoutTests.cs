using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Log;

public sealed class RecordTimeoutTests
{
    private static readonly TimeSpan Ms = TimeSpan.FromMilliseconds(1);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(1500)]
    [InlineData(30_000)]
    public void A_whole_millisecond_remainder_is_used_as_it_is(int milliseconds)
    {
        var timeout = RecordTimeout.FromRemaining(TimeSpan.FromMilliseconds(milliseconds));

        Assert.True(timeout.RecordRuns);
        Assert.Equal(milliseconds, timeout.Milliseconds);
    }

    [Fact]
    public void A_remainder_under_1_ms_runs_with_1_ms_never_0()
    {
        foreach (var remaining in new[] { TimeSpan.FromTicks(1), TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2), Ms - TimeSpan.FromTicks(1) })
        {
            var timeout = RecordTimeout.FromRemaining(remaining);

            Assert.True(timeout.RecordRuns);
            Assert.Equal(1, timeout.Milliseconds);
        }
    }

    [Fact]
    public void A_fraction_of_a_millisecond_rounds_up()
    {
        Assert.Equal(2, RecordTimeout.FromRemaining(Ms + TimeSpan.FromTicks(1)).Milliseconds);
        Assert.Equal(2, RecordTimeout.FromRemaining(Ms * 1.5).Milliseconds);
        Assert.Equal(2, RecordTimeout.FromRemaining((Ms * 2) - TimeSpan.FromTicks(1)).Milliseconds);
        Assert.Equal(2, RecordTimeout.FromRemaining(Ms * 2).Milliseconds);
        Assert.Equal(1235, RecordTimeout.FromRemaining(TimeSpan.FromMilliseconds(1234) + TimeSpan.FromTicks(1)).Milliseconds);
    }

    [Fact]
    public void When_nothing_remains_no_record_runs()
    {
        Assert.False(RecordTimeout.FromRemaining(TimeSpan.Zero).RecordRuns);
        Assert.Null(RecordTimeout.FromRemaining(TimeSpan.Zero).Milliseconds);
        Assert.False(RecordTimeout.FromRemaining(TimeSpan.FromTicks(-1)).RecordRuns);
        Assert.False(RecordTimeout.FromRemaining(TimeSpan.FromSeconds(-30)).RecordRuns);
        Assert.False(RecordTimeout.FromRemaining(TimeSpan.MinValue).RecordRuns);
    }

    [Fact]
    public void The_answer_without_a_record_is_unconfirmed_timeout()
    {
        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), RecordTimeout.TimedOut);
    }

    [Fact]
    public void A_remainder_beyond_what_PostgreSQL_accepts_is_clamped_to_its_maximum()
    {
        Assert.Equal(int.MaxValue, RecordTimeout.MaxMilliseconds);
        Assert.Equal(int.MaxValue, RecordTimeout.FromRemaining(TimeSpan.MaxValue).Milliseconds);
        Assert.Equal(int.MaxValue, RecordTimeout.FromRemaining(TimeSpan.FromDays(30)).Milliseconds);
        Assert.Equal(int.MaxValue, RecordTimeout.FromRemaining(TimeSpan.FromMilliseconds(int.MaxValue) + TimeSpan.FromTicks(1)).Milliseconds);
        Assert.Equal(int.MaxValue, RecordTimeout.FromRemaining(TimeSpan.FromMilliseconds(int.MaxValue)).Milliseconds);
        Assert.Equal(int.MaxValue - 1, RecordTimeout.FromRemaining(TimeSpan.FromMilliseconds(int.MaxValue - 1)).Milliseconds);
    }
}
