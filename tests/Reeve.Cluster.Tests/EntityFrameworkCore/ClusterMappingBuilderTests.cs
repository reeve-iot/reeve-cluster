using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;

namespace Unit.EntityFrameworkCore;

public sealed class ClusterMappingBuilderTests
{
    private class Setting
    {
        public string Name { get; set; } = "";

        public string Value { get; set; } = "";

        public int Count { get; set; }
    }

    private sealed class SpecialSetting : Setting;

    private sealed class Other;

    [Fact]
    public void A_mapping_with_a_key_and_columns_builds_and_is_found_by_its_entity_type()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Setting>("setting").Key(s => s.Name).Columns(s => s.Value, s => s.Count));

        var map = Assert.IsType<EntityMapping>(mapping.Find(typeof(Setting)));
        Assert.Equal("setting", map.AggregateType);
        Assert.Equal("k1", map.Key!(new Setting { Name = "k1" }));
        Assert.Equal(new HashSet<string> { "Value", "Count" }, map.CountedProperties);
        Assert.Null(mapping.Find(typeof(Other)));
    }

    [Fact]
    public void NoKey_and_AllColumns_are_the_other_choice_and_leave_no_key_and_no_column_set()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Setting>("setting").NoKey().AllColumns());

        var map = Assert.IsType<EntityMapping>(mapping.Find(typeof(Setting)));
        Assert.Null(map.Key);
        Assert.Null(map.CountedProperties);
    }

    [Fact]
    public void Columns_by_name_names_a_shadow_property()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Setting>("setting").NoKey().Columns("Shadow"));

        Assert.Equal(new HashSet<string> { "Shadow" }, mapping.Find(typeof(Setting))!.CountedProperties);
    }

    [Fact]
    public void A_derived_entity_type_is_the_mapping_of_its_mapped_base()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<Setting>("setting").NoKey().AllColumns());

        Assert.Same(mapping.Find(typeof(Setting)), mapping.Find(typeof(SpecialSetting)));
    }

    [Fact]
    public void A_mapping_without_a_key_choice_is_refused_naming_the_type()
    {
        var error = Assert.Throws<InvalidOperationException>(() => ClusterMapping.Build(b => b.Entity<Setting>("setting").AllColumns()));

        Assert.Contains("Setting", error.Message, StringComparison.Ordinal);
        Assert.Contains("NoKey", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_mapping_without_a_columns_choice_is_refused_naming_the_type()
    {
        var error = Assert.Throws<InvalidOperationException>(() => ClusterMapping.Build(b => b.Entity<Setting>("setting").NoKey()));

        Assert.Contains("Setting", error.Message, StringComparison.Ordinal);
        Assert.Contains("AllColumns", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_choice_made_twice_is_refused()
    {
        Assert.Throws<InvalidOperationException>(() => ClusterMapping.Build(b => b.Entity<Setting>("s").NoKey().Key(s => s.Name).AllColumns()));
        Assert.Throws<InvalidOperationException>(() => ClusterMapping.Build(b => b.Entity<Setting>("s").NoKey().AllColumns().Columns(s => s.Value)));
    }

    [Fact]
    public void An_entity_type_mapped_twice_is_refused()
    {
        Assert.Throws<InvalidOperationException>(() =>
            ClusterMapping.Build(b =>
            {
                b.Entity<Setting>("a").NoKey().AllColumns();
                b.Entity<Setting>("b").NoKey().AllColumns();
            }));
    }

    [Fact]
    public void Two_entity_types_may_share_an_aggregate_type()
    {
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<Setting>("role").NoKey().AllColumns();
            b.Entity<Other>("role").NoKey().AllColumns();
        });

        Assert.Equal(2, mapping.Entities.Count);
    }

    [Fact]
    public void Columns_that_name_nothing_or_a_non_member_are_refused()
    {
        Assert.Throws<ArgumentException>(() => ClusterMapping.Build(b => b.Entity<Setting>("s").NoKey().Columns(Array.Empty<string>())));
        Assert.Throws<ArgumentException>(() => ClusterMapping.Build(b => b.Entity<Setting>("s").NoKey().Columns(" ")));
        Assert.Throws<ArgumentException>(() => ClusterMapping.Build(b => b.Entity<Setting>("s").NoKey().Columns(s => s.Name.Length)));
        Assert.Throws<ArgumentException>(() => ClusterMapping.Build(b => b.Entity<Setting>(" ")));
    }

    [Fact]
    public void AddReeveClusterEntityFrameworkCore_registers_the_mapping_and_refuses_an_incomplete_one_at_registration()
    {
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddReeveClusterEntityFrameworkCore(b => b.Entity<Setting>("setting").AllColumns()));

        services.AddReeveClusterEntityFrameworkCore(b => b.Entity<Setting>("setting").NoKey().AllColumns());

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<IClusterMapping>().Find(typeof(Setting)));
    }
}
