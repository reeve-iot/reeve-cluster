using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.Owned;

namespace Unit.Owned;

public sealed class PlacementTests
{
    private static readonly PlacedKind Widget = new("widget", EligibilityFilter.Any);

    private static PlacedKind Unit(string kind) => new(kind, EligibilityFilter.Any);

    private static PlacementMember Member(string id, long epoch = 1, params string[] capabilities) =>
        new(id, epoch, [.. capabilities.Length == 0 ? ["unit:widget", "unit:k0", "unit:k1", "unit:k2", "unit:k3"] : capabilities]);

    private static PlacedRow Owned(string kind, string owner, long token = 10, bool holdValid = true) =>
        new(kind, kind, new OwnedRow(owner, 1, token), holdValid);

    private static PlacedRow Free(string kind, long token = 10) => new(kind, kind, new OwnedRow(null, null, token), false);

    private static IReadOnlyList<Assignment> Plan(
        IReadOnlyList<PlacedKind> kinds, IReadOnlyList<PlacedRow> rows, IReadOnlyList<PlacementMember> members, params Handover[] handovers) =>
        Placement.Plan(kinds, rows, members, handovers.ToDictionary(h => (h.Kind, h.Key)));

    private static UnitKind Registered(string kind, AdoptionPolicy policy = AdoptionPolicy.Placer, bool keyed = false) =>
        new(kind, typeof(object), new UnitKindOptions { Policy = policy }) { Keyed = keyed };

    [Fact]
    public void Only_units_with_the_Placer_policy_are_placed_never_a_keyed_kind_nor_the_placer_itself()
    {
        var placed = Placement.PlacedKinds(
        [
            Registered("widget"),
            Registered("stream", keyed: true),
            Registered("adopted", AdoptionPolicy.AnyEligible),
            Registered("waits", AdoptionPolicy.None),
            Registered(Placement.PlacerKind),
        ]);

        Assert.Equal(["widget"], placed.Select(k => k.Kind));
    }

    [Fact]
    public void A_placed_kind_registers_the_placers_own_kind_once_with_the_None_policy_and_its_name_is_refused()
    {
        var services = new ServiceCollection();
        services.AddClusterKeyedResource<WidgetProbe>("adopted", o => o.Policy = AdoptionPolicy.AnyEligible);
        Assert.DoesNotContain(services, d => d.ImplementationInstance is UnitKind { Kind: Placement.PlacerKind });

        services.AddClusterUnit<WidgetProbe>("widget");
        services.AddClusterUnit<WidgetProbe>("other");

        var placer = Assert.Single(services, d => d.ImplementationInstance is UnitKind { Kind: Placement.PlacerKind });
        Assert.Equal((typeof(PlacerUnit), AdoptionPolicy.None), (((UnitKind)placer.ImplementationInstance!).UnitType, ((UnitKind)placer.ImplementationInstance!).Options.Policy));
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddClusterUnit<WidgetProbe>(Placement.PlacerKind));
    }

    [Fact]
    public void An_absent_unit_is_inserted_for_its_one_key_the_kind_name()
    {
        var assignment = Assert.Single(Plan([Widget], [], [Member("a")]));

        Assert.Equal(("widget", "widget", null, "a"), (assignment.Kind, assignment.Key, assignment.Expected, assignment.Target.ReplicaId));
    }

    [Fact]
    public void A_free_or_orphaned_unit_is_assigned_as_read()
    {
        Assert.Equal(new OwnedRow(null, null, 10), Assert.Single(Plan([Widget], [Free("widget")], [Member("a")])).Expected);
        Assert.Equal(new OwnedRow("gone", 1, 10), Assert.Single(Plan([Widget], [Owned("widget", "gone", holdValid: false)], [Member("a")])).Expected);
    }

    [Fact]
    public void A_row_with_a_valid_hold_is_never_assigned()
    {
        Assert.Empty(Plan([Widget], [Owned("widget", "b")], [Member("a"), Member("b")]));
    }

    [Fact]
    public void Rows_of_kinds_the_placer_does_not_place_are_left_alone()
    {
        Assert.DoesNotContain(Plan([Widget], [Free("other")], [Member("a", 1, "unit:other", "unit:widget")]), a => a.Kind == "other");
    }

    [Fact]
    public void Only_a_member_advertising_the_kind_and_passing_its_filter_is_a_target()
    {
        var filtered = Widget with { Eligibility = EligibilityFilter.Any.Require("gpu").Forbid("degraded") };
        var members = new[]
        {
            Member("a", 1, "gpu"),
            Member("b", 1, "unit:widget"),
            Member("c", 1, "unit:widget", "gpu", "degraded"),
            Member("d", 1, "unit:widget", "gpu"),
        };

        Assert.Equal("d", Assert.Single(Plan([filtered], [], members)).Target.ReplicaId);
    }

    [Fact]
    public void No_eligible_member_leaves_the_row_unassigned()
    {
        Assert.Empty(Plan([Widget], [Free("widget")], [Member("a", 1, "unit:other")]));
        Assert.Empty(Plan([Widget], [], []));
    }

    [Fact]
    public void Each_unit_goes_to_the_least_loaded_member_counting_this_pass_ties_by_replica_id()
    {
        var plan = Plan([Unit("k0"), Unit("k1"), Unit("k2"), Unit("k3")], [Owned("k0", "a")], [Member("c"), Member("a"), Member("b")]);

        Assert.Equal([("k1", "b"), ("k2", "c"), ("k3", "a")], plan.Select(p => (p.Key, p.Target.ReplicaId)));
    }

    [Fact]
    public void Only_valid_holds_count_as_load()
    {
        var plan = Plan([Unit("k0"), Unit("k1"), Unit("k2")], [Owned("k0", "a", holdValid: false), Owned("k1", "b")], [Member("a"), Member("b")]);

        Assert.Equal([("k0", "a"), ("k2", "a")], plan.Select(p => (p.Key, p.Target.ReplicaId)));
    }

    [Fact]
    public void A_handed_over_row_avoids_its_old_owner_unless_it_is_the_only_target()
    {
        var handover = new Handover("widget", "widget", 10, "a");

        Assert.Equal("b", Assert.Single(Plan([Widget], [Free("widget")], [Member("a"), Member("b")], handover)).Target.ReplicaId);
        Assert.Equal("a", Assert.Single(Plan([Widget], [Free("widget")], [Member("a")], handover)).Target.ReplicaId);
    }

    [Fact]
    public void The_target_carries_the_member_epoch_the_assignment_writes()
    {
        Assert.Equal(7, Assert.Single(Plan([Widget], [], [Member("a", 7)])).Target.Epoch);
    }

    [Fact]
    public void A_barred_member_is_not_assigned_the_key_even_when_it_is_the_only_target()
    {
        var barred = new HashSet<Barred> { new("widget", "widget", "a") };

        Assert.Equal("b", Assert.Single(Placement.Plan([Widget], [Free("widget")], [Member("a"), Member("b")], new Dictionary<(string, string), Handover>(), barred)).Target.ReplicaId);
        Assert.Empty(Placement.Plan([Widget], [Free("widget")], [Member("a")], new Dictionary<(string, string), Handover>(), barred));
        Assert.Equal("a", Assert.Single(Placement.Plan([Unit("k0")], [], [Member("a")], new Dictionary<(string, string), Handover>(), barred)).Target.ReplicaId);
    }

    [Fact]
    public void A_rebalance_moves_one_unit_off_the_most_loaded_member_when_another_holds_two_fewer()
    {
        PlacedKind[] units = [Unit("k0"), Unit("k1"), Unit("k2"), Unit("k3")];
        PlacedRow[] rows = [Owned("k0", "a", 1), Owned("k1", "a", 2), Owned("k2", "a", 3), Owned("k3", "b", 4)];

        Assert.Equal(new Handover("k0", "k0", 1, "a"), Placement.Rebalance(units, rows, [Member("a"), Member("b"), Member("c")]));
        Assert.Null(Placement.Rebalance(units, rows[1..], [Member("a"), Member("b")]));
    }

    [Fact]
    public void A_rebalance_never_moves_off_a_member_that_is_not_serving()
    {
        PlacedKind[] units = [Unit("k0"), Unit("k1")];
        PlacedRow[] rows = [Owned("k0", "a", 1), Owned("k1", "a", 2)];

        Assert.Equal("a", Placement.Rebalance(units, rows, [Member("a"), Member("b")])?.From);
        Assert.Null(Placement.Rebalance(units, rows, [Member("b")]));
    }

    [Fact]
    public void A_rebalance_moves_only_a_unit_a_lighter_member_may_take()
    {
        PlacedKind[] units = [Unit("k0"), Unit("k1"), Unit("k2")];
        PlacedRow[] rows = [Owned("k0", "a", 1), Owned("k1", "a", 2), Owned("k2", "a", 3)];
        var b = Member("b", 1, "unit:k2");

        Assert.Equal(new Handover("k2", "k2", 3, "a"), Placement.Rebalance(units, rows, [Member("a"), b]));
        Assert.Null(Placement.Rebalance(units, rows, [Member("a"), b], new HashSet<Barred> { new("k2", "k2", "b") }));
    }

    [Fact]
    public void A_rebalance_moves_a_unit_held_validly_never_a_row_whose_hold_is_not_valid()
    {
        PlacedKind[] units = [Unit("k0"), Unit("k1"), Unit("k2")];

        PlacedRow[] rows = [Owned("k0", "a", 1), Owned("k1", "a", 2), Owned("k2", "a", 3, holdValid: false)];

        Assert.Null(Placement.Rebalance(units, rows, [Member("a"), Member("b", 1, "unit:k2")]));
    }

    [Fact]
    public void A_rebalance_moves_off_the_most_loaded_member_first()
    {
        PlacedKind[] units = [Unit("k0"), Unit("k1"), Unit("k2"), Unit("k3"), Unit("k4"), Unit("k5"), Unit("k6")];
        PlacedRow[] rows =
        [
            Owned("k0", "b", 1), Owned("k1", "b", 2), Owned("k2", "b", 3),
            Owned("k3", "a", 4), Owned("k4", "a", 5), Owned("k5", "a", 6), Owned("k6", "a", 7),
        ];
        string[] all = ["unit:k0", "unit:k1", "unit:k2", "unit:k3", "unit:k4", "unit:k5", "unit:k6"];

        Assert.Equal(new Handover("k3", "k3", 4, "a"), Placement.Rebalance(units, rows, [Member("a", 1, all), Member("b", 1, all), Member("c", 1, all)]));
    }

    private sealed class WidgetProbe : IOwnedUnit
    {
        public Task OnAcquiredAsync(OwnedContext context, CancellationToken stop) => Task.CompletedTask;

        public Task OnLostAsync(OwnedContext context) => Task.CompletedTask;
    }
}
