using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Owned;

namespace Unit.Owned;

public sealed class PlacerMemoryTests
{
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Tick = TimeSpan.FromTicks(1);

    private readonly FakeTimeProvider time = new(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

    private static PlacementMember Member(string id, long epoch = 1) => new(id, epoch, ["unit:widget"]);

    private static PlacementRead Read(IReadOnlyList<PlacementMember> members, params PlacedRow[] rows) => new(members, rows, []);

    private static PlacedRow Row(string? owner, long token) =>
        new("widget", "widget", new OwnedRow(owner, owner is null ? null : 1, token), owner is not null);

    [Fact]
    public void A_new_tenure_rebalances_only_a_full_window_after_its_start()
    {
        var memory = new PlacerMemory(Window, time);
        memory.Observe(Read([Member("a")]));

        time.Advance(Window - Tick);
        Assert.False(memory.MayRebalance());
        time.Advance(Tick);
        Assert.True(memory.MayRebalance());
    }

    [Theory]
    [InlineData("join")]
    [InlineData("leave")]
    [InlineData("rejoin")]
    public void A_change_of_the_serving_members_restarts_the_window(string change)
    {
        var memory = new PlacerMemory(Window, time);
        memory.Observe(Read([Member("a"), Member("b")]));
        time.Advance(2 * Window);
        PlacementMember[] after = change switch
        {
            "join" => [Member("a"), Member("b"), Member("c")],
            "leave" => [Member("a")],
            _ => [Member("a"), Member("b", 2)],
        };

        memory.Observe(Read(after));

        time.Advance(Window - Tick);
        Assert.False(memory.MayRebalance());
        time.Advance(Tick);
        Assert.True(memory.MayRebalance());
    }

    [Fact]
    public void A_step_of_the_wall_clock_neither_ends_the_window_nor_lifts_a_bar()
    {
        var stepped = new SteppedTime(time);
        var memory = new PlacerMemory(Window, stepped);
        memory.Assigned(("widget", "widget"), 10, "a");
        memory.Observe(Read([Member("a")], Row(null, 10)));

        stepped.Step(2 * Window);
        memory.Observe(Read([Member("a")], Row(null, 10)));

        Assert.False(memory.MayRebalance());
        Assert.NotEmpty(memory.Barred);
    }

    [Fact]
    public void No_rebalance_while_a_requested_release_is_pending_and_it_ends_once_the_key_carries_another_tenure()
    {
        var memory = new PlacerMemory(Window, time);
        time.Advance(Window);
        memory.Observe(Read([Member("a"), Member("b")], Row("a", 10)));
        memory.Requested(new Handover("widget", "widget", 10, "a"));

        memory.Observe(Read([Member("a"), Member("b")], Row(null, 10)));
        Assert.False(memory.MayRebalance());

        memory.Observe(Read([Member("a"), Member("b")], Row("b", 11)));
        Assert.True(memory.MayRebalance());
    }

    [Fact]
    public void A_member_that_released_an_assignment_at_once_is_barred_from_the_key_for_one_window()
    {
        var memory = new PlacerMemory(Window, time);
        memory.Assigned(("widget", "widget"), 10, "a");

        memory.Observe(Read([Member("a")], Row(null, 10)));

        Assert.Equal([new Barred("widget", "widget", "a")], memory.Barred);
        time.Advance(Window - Tick);
        memory.Observe(Read([Member("a")], Row(null, 10)));
        Assert.NotEmpty(memory.Barred);
        time.Advance(Tick);
        memory.Observe(Read([Member("a")], Row(null, 10)));
        Assert.Empty(memory.Barred);
    }

    [Fact]
    public void A_release_the_placer_requested_or_by_a_member_no_longer_serving_bars_nobody()
    {
        var requested = new PlacerMemory(Window, time);
        requested.Assigned(("widget", "widget"), 10, "a");
        requested.Requested(new Handover("widget", "widget", 10, "a"));
        requested.Observe(Read([Member("a")], Row(null, 10)));

        var drained = new PlacerMemory(Window, time);
        drained.Assigned(("widget", "widget"), 10, "a");
        drained.Observe(Read([Member("b")], Row(null, 10)));

        Assert.Empty(requested.Barred);
        Assert.Empty(drained.Barred);
    }

    [Fact]
    public void An_assignment_whose_key_carries_another_tenure_is_forgotten()
    {
        var memory = new PlacerMemory(Window, time);
        memory.Assigned(("widget", "widget"), 10, "a");

        memory.Observe(Read([Member("a")], Row("b", 12)));
        memory.Observe(Read([Member("a")], Row(null, 12)));

        Assert.Empty(memory.Barred);
    }

    [Fact]
    public void A_failed_request_is_forgotten_unless_a_later_one_replaced_it()
    {
        var memory = new PlacerMemory(Window, time);
        time.Advance(Window);
        var first = new Handover("widget", "widget", 10, "a");
        memory.Requested(first);
        memory.Forget(first);
        Assert.True(memory.MayRebalance());

        memory.Requested(first);
        memory.Requested(first with { Token = 11 });
        memory.Forget(first);
        Assert.False(memory.MayRebalance());
    }

    private sealed class SteppedTime(TimeProvider inner) : TimeProvider
    {
        private TimeSpan step;

        public override long TimestampFrequency => inner.TimestampFrequency;

        public void Step(TimeSpan by) => step += by;

        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow() + step;

        public override long GetTimestamp() => inner.GetTimestamp();
    }
}
