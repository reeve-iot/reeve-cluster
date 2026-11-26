using Microsoft.Extensions.Options;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;

namespace Int.Ask;

public sealed class AskRelayWireTests : IClassFixture<RedisFixture>, IAsyncLifetime
{
    private static readonly TimeSpan Margin = TimeSpan.FromMilliseconds(300);

    private readonly RedisFixture redis;
    private ConnectionMultiplexer multiplexer = null!;
    private RedisAskWire wire = null!;
    private AskCells cells = null!;

    public AskRelayWireTests(RedisFixture redis)
    {
        this.redis = redis;
    }

    public async Task InitializeAsync()
    {
        multiplexer = await redis.ConnectAsync();
        wire = new RedisAskWire(multiplexer, Options.Create(new ClusterRedisOptions { KeyPrefix = "wire" + Guid.NewGuid().ToString("N") + ":" }));
        cells = new AskCells(wire, Margin, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
    }

    public async Task DisposeAsync() => await multiplexer.DisposeAsync();

    [Fact]
    public async Task A_replace_wins_only_against_the_value_it_expected_and_keeps_a_time_to_live()
    {
        var id = Guid.NewGuid();
        await wire.WriteAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), TimeSpan.FromSeconds(30), CancellationToken.None);
        var pending = AskCellCodec.Encode(AskCell.Pending);

        var first = await wire.ReplaceAsync(AskAddress.Record(id), pending, AskCellCodec.Encode(AskCell.Taken), TimeSpan.FromSeconds(30), CancellationToken.None);
        var second = await wire.ReplaceAsync(AskAddress.Record(id), pending, AskCellCodec.Encode(AskCell.Withdrawn), TimeSpan.FromSeconds(30), CancellationToken.None);
        var read = await wire.ReadAsync(AskAddress.Record(id), CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        Assert.Equal(AskCellKind.Taken, AskCellCodec.Decode(read.Value!)!.Kind);
        Assert.InRange(read.TimeToLive!.Value, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task A_record_expires_and_a_missing_one_cannot_be_taken_or_replaced()
    {
        var id = Guid.NewGuid();
        await wire.WriteAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), TimeSpan.FromMilliseconds(200), CancellationToken.None);
        await Task.Delay(500);

        Assert.Null((await wire.ReadAsync(AskAddress.Record(id), CancellationToken.None)).Value);
        Assert.Equal(TakeEnd.NotPending, await cells.TakeAsync(id, CancellationToken.None));
        Assert.False(await wire.ReplaceAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), AskCellCodec.Encode(AskCell.Taken), TimeSpan.FromSeconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task A_target_does_not_take_a_record_that_is_within_the_margin_of_its_end()
    {
        var id = Guid.NewGuid();
        await wire.WriteAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), TimeSpan.FromMilliseconds(250), CancellationToken.None);

        Assert.Equal(TakeEnd.TooLate, await cells.TakeAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task A_receipt_and_a_withdrawal_that_race_on_one_record_have_exactly_one_winner()
    {
        for (var round = 0; round < 200; round++)
        {
            var id = Guid.NewGuid();
            await wire.WriteAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), TimeSpan.FromSeconds(30), CancellationToken.None);

            var taking = Task.Run(() => cells.TakeAsync(id, CancellationToken.None));
            var withdrawing = Task.Run(() => cells.WithdrawAsync(id, CancellationToken.None));
            var take = await taking;
            var withdrawal = (await withdrawing)!.Value;

            if (take == TakeEnd.Written)
            {
                Assert.Equal(WithdrawKind.AlreadyTaken, withdrawal.Kind);
            }
            else
            {
                Assert.Equal(TakeEnd.NotPending, take);
                Assert.Equal(WithdrawKind.Withdrew, withdrawal.Kind);
            }
        }
    }

    [Fact]
    public async Task An_answer_is_written_only_to_a_taken_record_and_a_decline_only_to_a_pending_one()
    {
        var taken = Guid.NewGuid();
        var pending = Guid.NewGuid();
        await wire.WriteAsync(AskAddress.Record(taken), AskCellCodec.Encode(AskCell.Taken), TimeSpan.FromSeconds(30), CancellationToken.None);
        await wire.WriteAsync(AskAddress.Record(pending), AskCellCodec.Encode(AskCell.Pending), TimeSpan.FromSeconds(30), CancellationToken.None);
        var answer = new AskAnswer(AskAnswerKind.OwnerLost, default, null);

        Assert.Equal(AnswerEnd.Written, await cells.AnswerAsync(taken, answer, CancellationToken.None));
        Assert.Equal(AnswerEnd.Refused, await cells.AnswerAsync(pending, answer, CancellationToken.None));
        Assert.True(await cells.DeclineAsync(pending, DeclineReason.Stopping, CancellationToken.None));
        Assert.False(await cells.DeclineAsync(taken, DeclineReason.Stopping, CancellationToken.None));

        var (answered, _) = await cells.ReadAsync(taken, CancellationToken.None);
        var (declined, _) = await cells.ReadAsync(pending, CancellationToken.None);
        Assert.Equal(AskAnswerKind.OwnerLost, answered!.Answer!.Kind);
        Assert.Equal(DeclineReason.Stopping, declined!.Reason);
    }

    [Fact]
    public async Task A_message_reaches_the_process_it_is_addressed_to_and_no_other()
    {
        var mine = Guid.NewGuid();
        var other = Guid.NewGuid();
        var heard = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wrong = 0;
        await wire.SubscribeAsync(AskAddress.RequestChannel("a", mine), heard.SetResult, CancellationToken.None);
        await wire.SubscribeAsync(AskAddress.RequestChannel("a", other), _ => Interlocked.Increment(ref wrong), CancellationToken.None);

        await wire.PublishAsync(AskAddress.RequestChannel("a", mine), [1, 2, 3], CancellationToken.None);

        Assert.Equal(new byte[] { 1, 2, 3 }, await heard.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await Task.Delay(100);
        Assert.Equal(0, wrong);
    }
}
