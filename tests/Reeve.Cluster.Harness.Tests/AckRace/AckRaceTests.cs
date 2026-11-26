using Harness.SelfTests;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Stack;
using Xunit.Abstractions;

namespace Harness.AckRace;

[Collection(StackCollection.Name)]
public sealed class AckRaceTests(StackFixture stack, ITestOutputHelper output)
{
    public enum Actor
    {
        Record,
        Open,
    }

    public static TheoryData<string, string, Actor, Actor> Schedules()
    {
        var data = new TheoryData<string, string, Actor, Actor>();
        foreach (var record in new[] { LogFaultPoints.RecordBeforeUpsert, LogFaultPoints.RecordBetweenUpsertAndSkipRead, LogFaultPoints.RecordBeforeCommit })
        {
            foreach (var open in new[] { LogFaultPoints.OpenBeforeUpsert, LogFaultPoints.OpenAfterUpsert, LogFaultPoints.OpenAfterInsert })
            {
                foreach (var first in new[] { Actor.Record, Actor.Open })
                {
                    foreach (var releasedFirst in new[] { Actor.Record, Actor.Open })
                    {
                        data.Add(record.Name, open.Name, first, releasedFirst);
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Schedules))]
    public async Task I30_an_open_concurrent_with_a_record_is_either_refused_or_named(string recordPoint, string openPoint, Actor first, Actor releasedFirst)
    {
        await using var rig = await AckRaceRig.CreateAsync(stack, output.WriteLine);
        var seq = await rig.WriteAsync();

        using var recordHold = rig.Writer.Faults.HoldNext(Point(recordPoint));
        using var openHold = rig.Operator.Faults.HoldNext(Point(openPoint));

        Task<IReadOnlyList<string>>? record = null;
        Task<SkipOpenResult>? open = null;
        void Start(Actor actor)
        {
            if (actor == Actor.Record)
            {
                record = rig.RecordAsync(seq);
            }
            else
            {
                open = rig.OpenAsync(seq);
            }
        }

        Task Settle(Actor actor) => actor == Actor.Record
            ? rig.SettleAsync(record!, recordHold, rig.Writer)
            : rig.SettleAsync(open!, openHold, rig.Operator);
        HoldRule HoldOf(Actor actor) => actor == Actor.Record ? recordHold : openHold;

        var second = first == Actor.Record ? Actor.Open : Actor.Record;
        Start(first);
        await Settle(first);
        Start(second);
        await Settle(second);

        var releasedSecond = releasedFirst == Actor.Record ? Actor.Open : Actor.Record;
        if (HoldOf(releasedFirst).Release())
        {
            await Settle(releasedFirst);
            await Settle(releasedSecond);
        }

        HoldOf(releasedSecond).Release();
        recordHold.Dispose();
        openHold.Dispose();

        var skipped = await record!.WaitAsync(TimeSpan.FromSeconds(30));
        var opened = await open!.WaitAsync(TimeSpan.FromSeconds(30));
        output.WriteLine($"open: {opened}; record named: [{string.Join(", ", skipped)}]");

        Assert.True(
            opened is SkipOpenResult.Refused || skipped.Contains(AckRaceRig.SkipType),
            $"The open committed a range from {seq} over the acknowledged write, and the record named [{string.Join(", ", skipped)}].");
        if (opened is SkipOpenResult.Refused refused)
        {
            Assert.Equal(seq, refused.AckedSeq);
            Assert.Equal(0L, await rig.SkipCountAsync());
        }
    }

    [Fact]
    public async Task Replay_M1_ackrace_nolock_the_record_waits_for_the_open_that_read_acked_seq_first_and_names_its_type()
    {
        await using var rig = await AckRaceRig.CreateAsync(stack, output.WriteLine);
        using var openRead = rig.Operator.Faults.HoldNext(LogFaultPoints.OpenAfterUpsert);

        var open = rig.OpenAsync(1);
        await openRead.WaitReachedAsync();

        var seq = await rig.WriteAsync();
        Assert.Equal(1, seq);

        var record = rig.RecordAsync(seq);
        await rig.SettleAsync(record, null, rig.Writer);
        openRead.Release();

        Assert.IsType<SkipOpenResult.Opened>(await open.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal([AckRaceRig.SkipType], await record.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal([LogActions.OpenLock, LogActions.AckRecord, LogActions.AckSkipRead],
            rig.Order(LogActions.OpenLock, LogActions.AckRecord, LogActions.AckSkipRead));
    }

    [Fact]
    public async Task Replay_M1_ackrace_readfirst_the_skip_read_after_the_upsert_sees_the_open_that_held_the_row()
    {
        await using var rig = await AckRaceRig.CreateAsync(stack, output.WriteLine);
        using var openLock = rig.Operator.Faults.HoldNext(LogFaultPoints.OpenAfterUpsert);

        var open = rig.OpenAsync(1);
        await openLock.WaitReachedAsync();

        var seq = await rig.WriteAsync();
        var record = rig.RecordAsync(seq);

        await rig.SettleAsync(record, null, rig.Writer);
        Assert.False(record.IsCompleted);
        openLock.Release();

        Assert.IsType<SkipOpenResult.Opened>(await open.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal([AckRaceRig.SkipType], await record.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal([LogActions.OpenLock, LogActions.AckRecord, LogActions.AckSkipRead],
            rig.Order(LogActions.OpenLock, LogActions.AckRecord, LogActions.AckSkipRead));
    }

    [Fact]
    public async Task Replay_M1_D7_an_open_after_the_record_of_an_acknowledged_write_is_refused_whatever_its_type()
    {
        await using var rig = await AckRaceRig.CreateAsync(stack, output.WriteLine);
        var seq = await rig.WriteAsync();
        Assert.Empty(await rig.RecordAsync(seq));

        var refused = Assert.IsType<SkipOpenResult.Refused>(await rig.OpenAsync(seq));
        Assert.Equal(seq, refused.AckedSeq);
        Assert.Equal(0L, await rig.SkipCountAsync());
        Assert.Equal([LogActions.AckRecord, LogActions.AckSkipRead, LogActions.OpenLock, LogActions.OpenRefuse],
            rig.Order(LogActions.AckRecord, LogActions.AckSkipRead, LogActions.OpenLock, LogActions.OpenRefuse, LogActions.OpenCommit));
    }

    private static FaultPoint Point(string name) =>
        new[]
        {
            LogFaultPoints.RecordBeforeUpsert, LogFaultPoints.RecordBetweenUpsertAndSkipRead, LogFaultPoints.RecordBeforeCommit,
            LogFaultPoints.OpenBeforeUpsert, LogFaultPoints.OpenAfterUpsert, LogFaultPoints.OpenAfterInsert,
        }.Single(p => p.Name == name);
}
