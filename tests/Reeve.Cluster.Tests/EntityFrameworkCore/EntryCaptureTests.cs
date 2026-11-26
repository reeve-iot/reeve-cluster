using Microsoft.EntityFrameworkCore;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;

namespace Unit.EntityFrameworkCore;

public sealed class EntryCaptureTests
{
    [Fact]
    public void Modified_table_split_owned_one_captures_its_owner()
    {
        using var db = new OwnedOneDb();
        var owner = new TableSplitOwner { Id = 7, Name = "owner", Detail = new TableSplitDetail { Text = "before" } };
        db.Attach(owner);
        owner.Detail.Text = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<TableSplitOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));
    }

    [Fact]
    public void Added_and_removed_owned_many_elements_capture_the_owner_key()
    {
        using var db = new OwnedManyDb();
        var owner = new OwnedManyOwner { Id = 7, Name = "owner", Items = [new OwnedManyItem { Id = 3, Text = "old" }] };
        db.Attach(owner);
        owner.Items.RemoveAt(0);
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<OwnedManyOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));

        using var db2 = new OwnedManyDb();
        var owner2 = new OwnedManyOwner { Id = 7, Name = "owner" };
        db2.Attach(owner2);
        owner2.Items.Add(new OwnedManyItem { Text = "new" });
        db2.ChangeTracker.DetectChanges();
        AssertEntries(db2, ClusterMapping.Build(b => b.Entity<OwnedManyOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));
    }

    [Fact]
    public void Modified_json_owned_one_captures_its_owner()
    {
        using var db = new JsonOneDb();
        var owner = new JsonOwner { Id = 7, Name = "owner", Detail = new JsonDetail { Text = "before" } };
        db.Attach(owner);
        owner.Detail.Text = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<JsonOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));
    }

    [Fact]
    public void Removed_json_owned_many_element_captures_owner()
    {
        using var db = new JsonManyDb();
        var owner = new JsonManyOwner { Id = 7, Name = "owner" };
        var item = new JsonManyItem { Text = "old" };
        owner.Items.Add(item);
        db.Attach(owner);
        owner.Items.Remove(item);
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<JsonManyOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));
    }

    [Fact]
    public void Nested_owned_change_captures_root_owner()
    {
        using var db = new NestedDb();
        var owner = new NestedOwner { Id = 7, Name = "owner", Detail = new NestedDetail { Nested = new NestedValue { Text = "before" } } };
        db.Attach(owner);
        owner.Detail.Nested.Text = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<NestedOwner>("root").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("root", "7"));
    }

    [Fact]
    public void Owned_change_of_no_key_owner_captures_null_key()
    {
        using var db = new NoKeyOwnedDb();
        var owner = new NoKeyOwner { Id = 7, Name = "owner", Detail = new NoKeyDetail { Text = "before" } };
        db.Attach(owner);
        owner.Detail.Text = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<NoKeyOwner>("owner").NoKey().Columns(x => x.Name)), ("owner", null));
    }

    [Fact]
    public void Owned_change_counts_even_when_owner_columns_exclude_owned_data()
    {
        using var db = new ExcludedOwnedDb();
        var owner = new ExcludedOwner { Id = 7, Name = "owner", Detail = new ExcludedDetail { Text = "before" } };
        db.Attach(owner);
        owner.Detail.Text = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ExcludedOwner>("owner").Key(x => x.Id.ToString()).Columns(x => x.Name)), ("owner", "7"));
    }

    [Fact]
    public void Owned_change_under_unmapped_owner_is_ignored()
    {
        using var db = new UnmappedOwnedDb();
        var owner = new UnmappedOwner { Id = 7, Name = "owner", Detail = new UnmappedDetail { Text = "before" } };
        db.Attach(owner);
        owner.Detail.Text = "after";
        db.ChangeTracker.DetectChanges();
        
        var unrelated = new UnrelatedEntity { Id = 8 };
        db.Attach(unrelated);
        db.Entry(unrelated).State = EntityState.Modified;
        var mapping = ClusterMapping.Build(b => b.Entity<UnrelatedEntity>("unrelated").Key(x => x.Id.ToString()).AllColumns());
        AssertEntries(db, mapping, ("unrelated", "8"));
    }

    [Fact]
    public void Skip_navigation_link_addition_captures_both_mapped_sides()
    {
        using var db = new ManyDb();
        var left = new Left { Id = 1 };
        var right = new Right { Id = 2 };
        db.AttachRange(left, right);
        left.Rights.Add(right);
        AssertEntries(db, BothSides(), ("left", "1"), ("right", "2"));
    }

    [Fact]
    public void Skip_navigation_link_removal_captures_both_mapped_sides()
    {
        using var db = new ManyDb();
        var left = new Left { Id = 1 };
        var right = new Right { Id = 2 };
        left.Rights.Add(right);
        db.AttachRange(left, right);
        left.Rights.Remove(right);
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, BothSides(), ("left", "1"), ("right", "2"));
    }

    [Fact]
    public void Skip_navigation_link_captures_only_the_mapped_side()
    {
        using var db = new ManyDb();
        var left = new Left { Id = 1 };
        var right = new Right { Id = 2 };
        db.AttachRange(left, right);
        left.Rights.Add(right);
        AssertEntries(db, Map<Left>("left", x => x.Id), ("left", "1"));
    }

    [Fact]
    public void Cascade_delete_of_many_to_many_join_captures_both_mapped_sides_keyless()
    {
        using var db = new ManyDb();
        var left = new Left { Id = 1 };
        db.Attach(left);
        db.Entry(left).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<Left>("left").NoKey().AllColumns();
            b.Entity<Right>("right").NoKey().AllColumns();
        });
        AssertEntries(db, mapping, ("left", null), ("right", null));
    }

    [Fact]
    public void Cascade_paths_to_the_same_type_emit_one_keyless_entry()
    {
        using var db = new DuplicateCascadeDb();
        var parent = new DuplicateCascadeParent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b => b.Entity<DuplicateCascadeChild>("child").NoKey().AllColumns());
        var actual = EntryCapture.Entries(db, mapping).Where(x => x.AggregateType == "child" && x.AggregateKey is null).ToList();
        Assert.Single(actual);
    }

    [Fact]
    public void Skip_navigation_with_untracked_principal_emits_null_key_for_that_side()
    {
        using var db = new ExplicitManyDb();
        db.Attach(new ExplicitLeft { Id = 1 });
        db.Add(new JoinClass { Id = 3, LeftId = 1, RightId = 999 });
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<ExplicitLeft>("left").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<ExplicitRight>("right").Key(x => x.Id.ToString()).AllColumns();
        });
        AssertEntries(db, mapping, ("left", "1"), ("right", null));
    }

    [Fact]
    public void Explicit_join_entity_link_captures_both_sides()
    {
        using var db = new ExplicitManyDb();
        var left = new ExplicitLeft { Id = 1 };
        var right = new ExplicitRight { Id = 2 };
        db.AttachRange(left, right);
        left.Rights.Add(right);
        AssertEntries(db, ClusterMapping.Build(b =>
        {
            b.Entity<ExplicitLeft>("left").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<ExplicitRight>("right").Key(x => x.Id.ToString()).AllColumns();
        }), ("left", "1"), ("right", "2"));
    }

    [Fact]
    public void Modified_join_foreign_key_captures_old_and_new_principals()
    {
        using var db = new ExplicitManyDb();
        db.Attach(new ExplicitLeft { Id = 1 });
        db.Attach(new ExplicitRight { Id = 10 });
        db.Attach(new ExplicitRight { Id = 20 });
        var membership = new JoinClass { Id = 3, LeftId = 1, RightId = 10 };
        db.Attach(membership);
        membership.RightId = 20;
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<ExplicitLeft>("left").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<ExplicitRight>("right").Key(x => x.Id.ToString()).AllColumns();
        });
        AssertEntries(db, mapping, ("left", "1"), ("right", "10"), ("right", "20"));
    }

    [Fact]
    public void Modified_composite_join_foreign_key_captures_original_principal_when_one_part_changes()
    {
        using var db = new CompositeManyDb();
        db.Attach(new CompositeLeft { Id = 1 });
        db.Attach(new CompositeRight { Id = 10, TenantId = 100 });
        db.Attach(new CompositeRight { Id = 20, TenantId = 100 });
        var link = new CompositeLink { Id = 3, LeftId = 1, RightId = 10, RightTenantId = 100 };
        db.Attach(link);
        link.RightId = 20;
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<CompositeLeft>("left").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<CompositeRight>("right").Key(x => x.Id.ToString()).AllColumns();
        });
        AssertEntries(db, mapping, ("left", "1"), ("right", "10"), ("right", "20"));
    }

    [Fact]
    public void Skip_navigation_resolves_byte_array_principal_key_by_content()
    {
        using var db = new ByteKeyManyDb();
        var left = new ByteKeyLeft { Id = [1, 2] };
        db.Attach(left);
        db.Attach(new ByteKeyRight { Id = 2 });
        db.Add(new ByteKeyJoin { Id = 3, LeftId = [1, 2], RightId = 2 });
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<ByteKeyLeft>("left").Key(x => Convert.ToHexString(x.Id)).AllColumns();
            b.Entity<ByteKeyRight>("right").Key(x => x.Id.ToString()).AllColumns();
        });
        AssertEntries(db, mapping, ("left", "0102"), ("right", "2"));
    }

    [Fact]
    public void Added_mapped_entry_uses_its_current_key()
    {
        using var db = new CaptureEntryDb();
        db.Add(new CaptureEntity { Id = 1, Name = "added", Value = "value" });
        AssertEntries(db, CaptureMapping(), ("capture", "added"));
    }

    [Fact]
    public void Modified_mapped_key_emits_current_and_original_keys()
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "before", Value = "value" };
        db.Attach(entity);
        entity.Name = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, CaptureMapping(), ("capture", "after"), ("capture", "before"));
    }

    [Fact]
    public void Deleted_mapped_entry_emits_current_and_original_keys_when_key_changed()
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "before", Value = "value" };
        db.Attach(entity);
        entity.Name = "after";
        db.ChangeTracker.DetectChanges();
        db.Entry(entity).State = EntityState.Deleted;
        AssertEntries(db, CaptureMapping(), ("capture", "after"), ("capture", "before"));
    }

    [Fact]
    public void Modified_entry_without_counted_column_change_is_ignored()
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "same", Value = "before" };
        db.Attach(entity);
        entity.Value = "after";
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b => b.Entity<CaptureEntity>("capture").Key(x => x.Name).Columns(x => x.Name));
        AssertEntries(db, mapping);
    }

    [Fact]
    public void Modified_counted_column_emits_mapped_entry()
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "before", Value = "value" };
        db.Attach(entity);
        entity.Name = "after";
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b => b.Entity<CaptureEntity>("capture").Key(x => x.Id.ToString()).Columns(x => x.Name));
        AssertEntries(db, mapping, ("capture", "1"));
    }

    [Fact]
    public void One_of_multiple_counted_columns_changed_emits_mapped_entry()
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "before", Value = "unchanged" };
        db.Attach(entity);
        entity.Name = "after";
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b => b.Entity<CaptureEntity>("capture").Key(x => x.Id.ToString()).Columns(x => x.Value, x => x.Name));
        AssertEntries(db, mapping, ("capture", "1"));
    }

    [Theory]
    [InlineData(EntityState.Added)]
    [InlineData(EntityState.Deleted)]
    public void Added_or_deleted_mapped_entity_counts_whatever_the_counted_columns(EntityState state)
    {
        using var db = new CaptureEntryDb();
        var entity = new CaptureEntity { Id = 1, Name = "n", Value = "v" };
        db.Entry(entity).State = state;
        var mapping = ClusterMapping.Build(b => b.Entity<CaptureEntity>("capture").Key(x => x.Id.ToString()).Columns(x => x.Value));
        AssertEntries(db, mapping, ("capture", "1"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Cascade_delete_captures_untracked_mapped_child_as_keyless(bool parentMapped)
    {
        using var db = new CascadeDb();
        var parent = new Parent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b =>
        {
            if (parentMapped) b.Entity<Parent>("parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<Child>("child").NoKey().AllColumns();
        });
        AssertEntries(db, mapping, ("child", null), parentMapped ? ("parent", "1") : null);
    }

    [Fact]
    public void Transitive_cascade_delete_captures_untracked_mapped_grandchild()
    {
        using var db = new TransitiveDb();
        var parent = new Parent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<Parent>("parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<Grandchild>("grandchild").NoKey().AllColumns();
        });
        AssertEntries(db, mapping, ("parent", "1"), ("grandchild", null));
    }

    [Fact]
    public void Set_null_cascade_capture_respects_all_columns_and_counted_foreign_key()
    {
        using var db = new SetNullDb();
        var parent = new Parent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var all = ClusterMapping.Build(b =>
        {
            b.Entity<Parent>("parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<Dependent>("dependent").NoKey().AllColumns();
        });
        AssertEntries(db, all, ("parent", "1"), ("dependent", null));

        using var db2 = new SetNullDb();
        var parent2 = new Parent { Id = 1 };
        db2.Attach(parent2);
        db2.Entry(parent2).State = EntityState.Deleted;
        var excluded = ClusterMapping.Build(b =>
        {
            b.Entity<Parent>("parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<Dependent>("dependent").NoKey().Columns(x => x.Value);
        });
        AssertEntries(db2, excluded, ("parent", "1"));
    }

    [Fact]
    public void Set_null_fk_declared_on_base_captures_mapped_derived_dependent()
    {
        using var db = new DerivedSetNullDb();
        var parent = new DerivedFkParent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b => b.Entity<DerivedDependent>("derived").NoKey().AllColumns());
        AssertEntries(db, mapping, ("derived", null));
    }

    [Fact]
    public void Set_null_composite_fk_captures_when_one_foreign_key_column_is_counted()
    {
        using var db = new CompositeSetNullDb();
        var parent = new CompositeSetNullParent { Id = 1, TenantId = 2 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b => b.Entity<CompositeSetNullDependent>("dependent").NoKey().Columns(x => x.ParentId));
        AssertEntries(db, mapping, ("dependent", null));
    }

    [Fact]
    public void Owned_navigation_declared_on_base_captures_mapped_derived_owner()
    {
        using var db = new DerivedOwnedDb();
        var owner = new LightDevice { Id = 2, Name = "light", Settings = new DeviceSettings { Value = "before" } };
        db.Attach(owner);
        owner.Settings.Value = "after";
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b => b.Entity<LightDevice>("light").Key(x => x.Id.ToString()).Columns(x => x.Name));
        AssertEntries(db, mapping, ("light", "2"));
    }

    [Fact]
    public void Skip_navigation_declared_on_base_captures_mapped_derived_principal()
    {
        using var db = new DerivedManyDb();
        var group = new PriorityGroup { Id = 3 };
        var right = new BaseRight { Id = 4 };
        db.AttachRange(group, right);
        group.Rights.Add(right);
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<PriorityGroup>("group").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<BaseRight>("right").Key(x => x.Id.ToString()).AllColumns();
        });
        AssertEntries(db, mapping, ("group", "3"), ("right", "4"));
    }

    [Theory]
    [InlineData(DeleteBehavior.Restrict)]
    [InlineData(DeleteBehavior.ClientSetNull)]
    [InlineData(DeleteBehavior.NoAction)]
    public void Non_database_cascade_does_not_capture_dependent(DeleteBehavior behavior)
    {
        using DbContext db = behavior switch
        {
            DeleteBehavior.Restrict => new RestrictDb(),
            DeleteBehavior.ClientSetNull => new ClientSetNullDb(),
            DeleteBehavior.NoAction => new NoActionDb(),
            _ => throw new ArgumentOutOfRangeException(nameof(behavior))
        };
        Assert.Equal(behavior, db.Model.FindEntityType(typeof(Dependent))!.GetForeignKeys().Single().DeleteBehavior);
        var parent = new Parent { Id = 1 };
        db.Attach(parent);
        db.Entry(parent).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<Parent>("parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<Dependent>("dependent").NoKey().AllColumns();
        });
        AssertEntries(db, mapping, ("parent", "1"));
    }

    [Fact]
    public void Self_referencing_cascade_terminates_and_captures_keyless_type()
    {
        using var db = new SelfCascadeDb();
        var node = new Node { Id = 1 };
        db.Attach(node);
        db.Entry(node).State = EntityState.Deleted;
        var mapping = ClusterMapping.Build(b => b.Entity<Node>("node").Key(x => x.Id.ToString()).AllColumns());
        AssertEntries(db, mapping, ("node", "1"), ("node", null));
    }

    [Fact]
    public void Modified_complex_member_with_all_columns_is_captured()
    {
        using var db = new ComplexDb();
        var owner = new ComplexOwner { Id = 1, Address = new Address { City = "A" } };
        db.Attach(owner);
        owner.Address.City = "B";
        db.Entry(owner).ComplexProperty(x => x.Address).Property(x => x.City).IsModified = true;
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexOwner>("owner").Key(x => x.Id.ToString()).AllColumns()), ("owner", "1"));
    }

    [Fact]
    public void Modified_complex_member_named_as_a_column_is_captured()
    {
        using var db = new ComplexDb();
        var owner = new ComplexOwner { Id = 1, Address = new Address { City = "A" } };
        db.Attach(owner);
        owner.Address.City = "B";
        db.Entry(owner).ComplexProperty(x => x.Address).Property(x => x.City).IsModified = true;
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexOwner>("owner").Key(x => x.Id.ToString()).Columns("Address")), ("owner", "1"));
    }

    [Fact]
    public void Modified_complex_collection_named_as_a_column_is_captured()
    {
        using var db = new ComplexCollectionDb();
        var owner = new ComplexCollectionOwner { Id = 4 };
        db.Attach(owner);
        owner.Rules.Add(new Rule { Value = "new" });
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexCollectionOwner>("owner").Key(x => x.Id.ToString()).Columns("Rules")), ("owner", "4"));
    }

    [Fact]
    public void Removed_json_complex_collection_element_named_as_a_column_is_captured()
    {
        using var db = new ComplexCollectionDb();
        var owner = new ComplexCollectionOwner { Id = 6, Rules = [new Rule { Value = "existing" }] };
        db.Attach(owner);
        owner.Rules.RemoveAt(0);
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexCollectionOwner>("owner").Key(x => x.Id.ToString()).Columns("Rules")), ("owner", "6"));
    }

    [Fact]
    public void Modified_nested_complex_collection_counts_its_complex_parent()
    {
        using var db = new NestedComplexCollectionDb();
        var owner = new NestedComplexCollectionOwner { Id = 5, Settings = new Settings { Rules = [new Rule { Value = "before" }] } };
        db.Attach(owner);
        owner.Settings.Rules[0].Value = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<NestedComplexCollectionOwner>("owner").Key(x => x.Id.ToString()).Columns("Settings")), ("owner", "5"));
    }

    [Fact]
    public void Modified_one_of_multiple_nested_complex_properties_counts_parent()
    {
        using var db = new ComplexTreeDb();
        var owner = new ComplexTreeOwner { Id = 9, Settings = new ComplexTree { First = new Address { City = "before" }, Second = new Address { City = "same" } } };
        db.Attach(owner);
        owner.Settings.First.City = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexTreeOwner>("tree").Key(x => x.Id.ToString()).Columns("Settings")), ("tree", "9"));
    }

    [Fact]
    public void Modified_one_of_multiple_nested_complex_collections_counts_parent()
    {
        using var db = new ComplexTreeDb();
        var owner = new ComplexTreeOwner { Id = 9, Settings = new ComplexTree { Rules = [new Rule { Value = "before", Code = "same" }], OtherRules = [new Rule { Value = "unchanged" }] } };
        db.Attach(owner);
        owner.Settings.Rules[0].Value = "after";
        db.ChangeTracker.DetectChanges();
        AssertEntries(db, ClusterMapping.Build(b => b.Entity<ComplexTreeOwner>("tree").Key(x => x.Id.ToString()).Columns("Settings")), ("tree", "9"));
    }

    [Fact]
    public void Unchanged_complex_collection_does_not_count_when_another_column_changes()
    {
        using var db = new NestedComplexCollectionDb();
        var owner = new NestedComplexCollectionOwner { Id = 5, Name = "before", Settings = new Settings { Rules = [new Rule { Value = "same", Code = "same" }] } };
        db.Attach(owner);
        owner.Name = "after";
        db.ChangeTracker.DetectChanges();
        var mapping = ClusterMapping.Build(b => b.Entity<NestedComplexCollectionOwner>("owner").Key(x => x.Id.ToString()).Columns("Settings"));
        AssertEntries(db, mapping);
    }

    private static ClusterMapping Map<TEntity>(string aggregate, Func<TEntity, int>? key) where TEntity : class => ClusterMapping.Build(b =>
    {
        var entity = b.Entity<TEntity>(aggregate);
        if (key is null) entity.NoKey(); else entity.Key(x => key(x).ToString());
        entity.AllColumns();
    });

    private static ClusterMapping BothSides() => ClusterMapping.Build(b =>
    {
        b.Entity<Left>("left").Key(x => x.Id.ToString()).AllColumns();
        b.Entity<Right>("right").Key(x => x.Id.ToString()).AllColumns();
    });

    private static ClusterMapping CaptureMapping() => ClusterMapping.Build(b => b.Entity<CaptureEntity>("capture").Key(x => x.Name).AllColumns());

    private static void AssertEntries(DbContext db, IClusterMapping mapping, params (string Aggregate, string? Key)?[] expected)
    {
        var actual = EntryCapture.Entries(db, mapping).Select(x => (x.AggregateType, x.AggregateKey)).ToHashSet();
        Assert.Equal(expected.Where(x => x is not null).Select(x => (x!.Value.Aggregate, x.Value.Key)).ToHashSet(), actual);
    }

    private class TableSplitOwner { public int Id { get; set; } public string Name { get; set; } = ""; public TableSplitDetail Detail { get; set; } = new(); }
    private class TableSplitDetail { public string Text { get; set; } = ""; }
    private class OwnedManyOwner { public int Id { get; set; } public string Name { get; set; } = ""; public List<OwnedManyItem> Items { get; set; } = []; }
    private class OwnedManyItem { public int Id { get; set; } public string Text { get; set; } = ""; }
    private class JsonOwner { public int Id { get; set; } public string Name { get; set; } = ""; public JsonDetail Detail { get; set; } = new(); }
    private class JsonDetail { public string Text { get; set; } = ""; }
    private class JsonManyOwner { public int Id { get; set; } public string Name { get; set; } = ""; public List<JsonManyItem> Items { get; set; } = []; }
    private class JsonManyItem { public string Text { get; set; } = ""; }
    private class NestedOwner { public int Id { get; set; } public string Name { get; set; } = ""; public NestedDetail Detail { get; set; } = new(); }
    private class NestedDetail { public NestedValue Nested { get; set; } = new(); }
    private class NestedValue { public string Text { get; set; } = ""; }
    private class NoKeyOwner { public int Id { get; set; } public string Name { get; set; } = ""; public NoKeyDetail Detail { get; set; } = new(); }
    private class NoKeyDetail { public string Text { get; set; } = ""; }
    private class ExcludedOwner { public int Id { get; set; } public string Name { get; set; } = ""; public ExcludedDetail Detail { get; set; } = new(); }
    private class ExcludedDetail { public string Text { get; set; } = ""; }
    private class UnmappedOwner { public int Id { get; set; } public string Name { get; set; } = ""; public UnmappedDetail Detail { get; set; } = new(); }
    private class UnmappedDetail { public string Text { get; set; } = ""; }
    private class UnrelatedEntity { public int Id { get; set; } }
    private class CaptureEntity { public int Id { get; set; } public string Name { get; set; } = ""; public string Value { get; set; } = ""; }
    private class ComplexOwner { public int Id { get; set; } public Address Address { get; set; } = new(); }
    private class ComplexCollectionOwner { public int Id { get; set; } public List<Rule> Rules { get; set; } = []; }
    private class NestedComplexCollectionOwner { public int Id { get; set; } public string Name { get; set; } = ""; public Settings Settings { get; set; } = new(); }
    private class Settings { public List<Rule> Rules { get; set; } = []; }
    private class Rule { public string Value { get; set; } = ""; public string Code { get; set; } = ""; }
    private class ComplexTreeOwner { public int Id { get; set; } public ComplexTree Settings { get; set; } = new(); }
    private class ComplexTree { public Address First { get; set; } = new(); public Address Second { get; set; } = new(); public List<Rule> Rules { get; set; } = []; public List<Rule> OtherRules { get; set; } = []; }
    private class DuplicateCascadeParent { public int Id { get; set; } }
    private class DuplicateCascadeChild { public int Id { get; set; } public int FirstParentId { get; set; } public int SecondParentId { get; set; } }
    private class CompositeSetNullParent { public int Id { get; set; } public int TenantId { get; set; } }
    private class CompositeSetNullDependent { public int Id { get; set; } public int? ParentId { get; set; } public int? ParentTenantId { get; set; } }
    private class Left { public int Id { get; set; } public List<Right> Rights { get; set; } = []; }
    private class Right { public int Id { get; set; } public List<Left> Lefts { get; set; } = []; }
    private class ExplicitLeft { public int Id { get; set; } public List<ExplicitRight> Rights { get; set; } = []; }
    private class ExplicitRight { public int Id { get; set; } public List<ExplicitLeft> Lefts { get; set; } = []; }
    private class JoinClass { public int Id { get; set; } public int LeftId { get; set; } public int RightId { get; set; } }
    private class CompositeLeft { public int Id { get; set; } public List<CompositeRight> Rights { get; set; } = []; }
    private class CompositeRight { public int Id { get; set; } public int TenantId { get; set; } public List<CompositeLeft> Lefts { get; set; } = []; }
    private class CompositeLink { public int Id { get; set; } public int LeftId { get; set; } public int RightId { get; set; } public int RightTenantId { get; set; } }
    private class ByteKeyLeft { public byte[] Id { get; set; } = []; public List<ByteKeyRight> Rights { get; set; } = []; }
    private class ByteKeyRight { public int Id { get; set; } public List<ByteKeyLeft> Lefts { get; set; } = []; }
    private class ByteKeyJoin { public int Id { get; set; } public byte[] LeftId { get; set; } = []; public int RightId { get; set; } }
    private class Parent { public int Id { get; set; } public List<Child> Children { get; set; } = []; public List<Dependent> Dependents { get; set; } = []; }
    private class Child { public int Id { get; set; } public int ParentId { get; set; } public Parent Parent { get; set; } = null!; public List<Grandchild> Grandchildren { get; set; } = []; }
    private class Grandchild { public int Id { get; set; } public int ChildId { get; set; } public Child Child { get; set; } = null!; }
    private class Dependent { public int Id { get; set; } public int? ParentId { get; set; } public string Value { get; set; } = ""; public Parent? Parent { get; set; } }
    private class Node { public int Id { get; set; } public int? ParentId { get; set; } public Node? Parent { get; set; } public List<Node> Children { get; set; } = []; }
    private class Address { public int Id { get; set; } public string City { get; set; } = ""; }
    private class DerivedFkParent { public int Id { get; set; } }
    private class DependentBase { public int Id { get; set; } public int? ParentId { get; set; } public DerivedFkParent? Parent { get; set; } }
    private class DerivedDependent : DependentBase { public string Value { get; set; } = ""; }
    private class BaseDevice { public int Id { get; set; } public string Name { get; set; } = ""; public DeviceSettings Settings { get; set; } = new(); }
    private class LightDevice : BaseDevice { }
    private class DeviceSettings { public string Value { get; set; } = ""; }
    private class BaseGroup { public int Id { get; set; } public List<BaseRight> Rights { get; set; } = []; }
    private class PriorityGroup : BaseGroup { }
    private class BaseRight { public int Id { get; set; } public List<BaseGroup> Groups { get; set; } = []; }

    private class OwnedOneDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<TableSplitOwner>().OwnsOne(x => x.Detail); }
    private class OwnedManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<OwnedManyOwner>().OwnsMany(x => x.Items); }
    private class JsonOneDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<JsonOwner>().OwnsOne(x => x.Detail, x => x.ToJson()); }
    private class JsonManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<JsonManyOwner>().OwnsMany(x => x.Items, x => x.ToJson()); }
    private class NestedDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { var detail = b.Entity<NestedOwner>().OwnsOne(x => x.Detail); detail.OwnsOne(x => x.Nested); } }
    private class NoKeyOwnedDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<NoKeyOwner>().OwnsOne(x => x.Detail); }
    private class ExcludedOwnedDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<ExcludedOwner>().OwnsOne(x => x.Detail); }
    private class UnmappedOwnedDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<UnmappedOwner>().OwnsOne(x => x.Detail); b.Entity<UnrelatedEntity>(); } }
    private class ManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<Left>().HasMany(x => x.Rights).WithMany(x => x.Lefts); }
    private class ExplicitManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<ExplicitLeft>().HasMany(x => x.Rights).WithMany(x => x.Lefts).UsingEntity<JoinClass>(r => r.HasOne<ExplicitRight>().WithMany().HasForeignKey(x => x.RightId), l => l.HasOne<ExplicitLeft>().WithMany().HasForeignKey(x => x.LeftId)); }
    private class CompositeManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<CompositeRight>().HasAlternateKey(x => new { x.Id, x.TenantId }); b.Entity<CompositeLeft>().HasMany(x => x.Rights).WithMany(x => x.Lefts).UsingEntity<CompositeLink>(r => r.HasOne<CompositeRight>().WithMany().HasForeignKey(x => new { x.RightId, x.RightTenantId }).HasPrincipalKey(x => new { x.Id, x.TenantId }), l => l.HasOne<CompositeLeft>().WithMany().HasForeignKey(x => x.LeftId)); } }
    private class ByteKeyManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<ByteKeyLeft>().HasMany(x => x.Rights).WithMany(x => x.Lefts).UsingEntity<ByteKeyJoin>(r => r.HasOne<ByteKeyRight>().WithMany().HasForeignKey(x => x.RightId), l => l.HasOne<ByteKeyLeft>().WithMany().HasForeignKey(x => x.LeftId)); }
    private class CascadeDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<Parent>().HasMany(x => x.Children).WithOne(x => x.Parent).HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.Cascade); }
    private class TransitiveDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<Parent>().HasMany(x => x.Children).WithOne(x => x.Parent).HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.Cascade); b.Entity<Child>().HasMany(x => x.Grandchildren).WithOne(x => x.Child).HasForeignKey(x => x.ChildId).OnDelete(DeleteBehavior.Cascade); } }
    private class SetNullDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<Parent>().HasMany(x => x.Dependents).WithOne(x => x.Parent).HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.SetNull); }
    private abstract class NonCascadeDb(DeleteBehavior behavior) : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<Parent>().HasMany(x => x.Dependents).WithOne(x => x.Parent).HasForeignKey(x => x.ParentId).OnDelete(behavior); }
    private class RestrictDb : NonCascadeDb { public RestrictDb() : base(DeleteBehavior.Restrict) { } }
    private class ClientSetNullDb : NonCascadeDb { public ClientSetNullDb() : base(DeleteBehavior.ClientSetNull) { } }
    private class NoActionDb : NonCascadeDb { public NoActionDb() : base(DeleteBehavior.NoAction) { } }
    private class SelfCascadeDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<Node>().HasMany(x => x.Children).WithOne(x => x.Parent).HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.Cascade); }
    private class ComplexDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { var owner = b.Entity<ComplexOwner>(); owner.ComplexProperty(x => x.Address); } }
    private class CaptureEntryDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<CaptureEntity>(); }
    private class ComplexCollectionDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<ComplexCollectionOwner>().ComplexCollection(x => x.Rules, rules => rules.ToJson()); }
    private class NestedComplexCollectionDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<NestedComplexCollectionOwner>().ComplexProperty(x => x.Settings, settings => { settings.ToJson(); settings.ComplexCollection(x => x.Rules); }); }
    private class ComplexTreeDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) => b.Entity<ComplexTreeOwner>().ComplexProperty(x => x.Settings, settings => { settings.ToJson(); settings.ComplexProperty(x => x.First); settings.ComplexProperty(x => x.Second); settings.ComplexCollection(x => x.Rules); settings.ComplexCollection(x => x.OtherRules); }); }
    private class DuplicateCascadeDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<DuplicateCascadeParent>().HasMany<DuplicateCascadeChild>().WithOne().HasForeignKey(x => x.FirstParentId).OnDelete(DeleteBehavior.Cascade); b.Entity<DuplicateCascadeParent>().HasMany<DuplicateCascadeChild>().WithOne().HasForeignKey(x => x.SecondParentId).OnDelete(DeleteBehavior.Cascade); } }
    private class CompositeSetNullDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<CompositeSetNullParent>().HasAlternateKey(x => new { x.Id, x.TenantId }); b.Entity<CompositeSetNullParent>().HasMany<CompositeSetNullDependent>().WithOne().HasForeignKey(x => new { x.ParentId, x.ParentTenantId }).HasPrincipalKey(x => new { x.Id, x.TenantId }).OnDelete(DeleteBehavior.SetNull); } }
    private class DerivedSetNullDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<DependentBase>().HasOne(x => x.Parent).WithMany().HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.SetNull); b.Entity<DerivedDependent>(); } }
    private class DerivedOwnedDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<BaseDevice>().OwnsOne(x => x.Settings); b.Entity<LightDevice>(); } }
    private class DerivedManyDb : DbContext { protected override void OnConfiguring(DbContextOptionsBuilder b) => b.UseNpgsql("Host=unused"); protected override void OnModelCreating(ModelBuilder b) { b.Entity<BaseGroup>().HasMany(x => x.Rights).WithMany(x => x.Groups); b.Entity<PriorityGroup>(); } }
}
