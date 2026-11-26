using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster.Slots;

namespace Unit.Slots;

public sealed class SlotRegistryTests
{
    private static readonly Guid First = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static readonly Guid Second = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider time = new();

    [Fact]
    public void An_entry_answers_to_its_token_only()
    {
        var registry = new SlotRegistry(time);
        registry.Record("k", "i", First, 4, time.GetTimestamp(), Ttl);

        Assert.Equal(4, registry.TryGet("k", "i", First)!.Epoch);
        Assert.Null(registry.TryGet("k", "i", Second));
        Assert.Null(registry.TryGet("k", "other", First));
    }

    [Fact]
    public void An_entry_leaves_when_its_deadline_passes()
    {
        var registry = new SlotRegistry(time);
        registry.Record("k", "i", First, 4, time.GetTimestamp(), Ttl);

        time.Advance(Ttl - TimeSpan.FromMilliseconds(1));
        Assert.NotNull(registry.TryGet("k", "i", First));
        Assert.Equal(1, registry.Live());

        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.Null(registry.TryGet("k", "i", First));
        Assert.Equal(0, registry.Live());
    }

    [Fact]
    public void A_renewal_moves_the_deadline_from_the_start_of_the_renewal()
    {
        var registry = new SlotRegistry(time);
        registry.Record("k", "i", First, 4, time.GetTimestamp(), Ttl);
        time.Advance(TimeSpan.FromSeconds(8));

        Assert.True(registry.Extend("k", "i", First, time.GetTimestamp(), Ttl));
        time.Advance(TimeSpan.FromSeconds(8));

        Assert.NotNull(registry.TryGet("k", "i", First));
        Assert.False(registry.Extend("k", "i", Second, time.GetTimestamp(), Ttl));
    }

    [Fact]
    public void A_release_with_a_token_removes_only_that_tokens_entry_and_one_without_removes_any()
    {
        var registry = new SlotRegistry(time);
        registry.Record("k", "i", Second, 4, time.GetTimestamp(), Ttl);

        registry.Remove("k", "i", First);
        Assert.NotNull(registry.TryGet("k", "i", Second));

        registry.Remove("k", "i", null);
        Assert.Null(registry.TryGet("k", "i", Second));
    }

    [Fact]
    public void A_new_tenure_of_the_same_id_replaces_the_entry()
    {
        var registry = new SlotRegistry(time);
        registry.Record("k", "i", First, 4, time.GetTimestamp(), Ttl);
        registry.Record("k", "i", Second, 5, time.GetTimestamp(), Ttl);

        Assert.Null(registry.TryGet("k", "i", First));
        Assert.Equal(5, registry.TryGet("k", "i", Second)!.Epoch);
    }
}
