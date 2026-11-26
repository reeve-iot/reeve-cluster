using Reeve.Cluster;
using Reeve.Cluster.Log;
using Unit.Log;

namespace Unit.Generations;

public sealed class DependencyPassTests
{
    private static readonly Guid Token1 = Guid.Parse("00000000-0000-0000-0000-000000000001");
    private static readonly Guid Token2 = Guid.Parse("00000000-0000-0000-0000-000000000002");

    private sealed class Declared(string type, string[] dependsOn) : IReconciler
    {
        public string AggregateType => type;

        IReadOnlyList<string> IReconciler.DependsOn => dependsOn;

        public Task<ReconcileResult> ReconcileAsync(ReconcileContext context, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileResult(type));
    }

    private static async Task<GenerationStore> PublishedAsync(params string[] edges)
    {
        var store = Create(edges);
        await store.PublishAsync(1, Token1, [.. store.AggregateTypes.Select(t => (t, (object)$"{t}1"))]);
        return store;
    }

    private static GenerationStore Create(params string[] edges) =>
        Stores.Create(ReconcilerGraph.Build([.. edges.Select(Parse)]));

    private static Declared Parse(string edge)
    {
        var parts = edge.Split(':');
        return new Declared(parts[0], parts.Length == 1 ? [] : parts[1].Split(','));
    }

    [Fact]
    public async Task BeginPass_adds_the_dependents_of_a_dirty_type()
    {
        var store = await PublishedAsync("a", "b:a");

        var pass = store.BeginPass(2, Token2, ["a"]);

        Assert.Equal(["a", "b"], pass.DirtyTypes);
    }

    [Fact]
    public async Task BeginPass_adds_the_dependents_of_a_dirty_type_along_a_chain()
    {
        var store = await PublishedAsync("a", "b:a", "c:b");

        Assert.Equal(["a", "b", "c"], store.BeginPass(2, Token2, ["a"]).DirtyTypes);
        Assert.Equal(["b", "c"], store.BeginPass(2, Token2, ["b"]).DirtyTypes);
        Assert.Equal(["c"], store.BeginPass(2, Token2, ["c"]).DirtyTypes);
    }

    [Fact]
    public async Task BeginPass_adds_a_type_when_only_one_of_its_dependencies_is_in_the_pass()
    {
        var store = await PublishedAsync("a", "b", "c:a,b");

        var pass = store.BeginPass(2, Token2, ["b"]);

        Assert.Equal(["b", "c"], pass.DirtyTypes);
        Assert.True(pass.HasDirtyDependency("c"));
    }

    [Fact]
    public async Task BeginPass_adds_each_type_of_a_diamond_once_in_declared_order()
    {
        var store = await PublishedAsync("a", "b:a", "c:a", "d:b,c");

        var pass = store.BeginPass(2, Token2, ["d", "a"]);

        Assert.Equal(["a", "b", "c", "d"], pass.DirtyTypes);
    }

    [Fact]
    public async Task BeginPass_leaves_a_dependent_out_when_its_dependency_is_not_given()
    {
        var store = await PublishedAsync("a", "b:a");

        Assert.Equal(["b"], store.BeginPass(2, Token2, ["b"]).DirtyTypes);
        Assert.Empty(store.BeginPass(2, Token2, []).DirtyTypes);
    }

    [Fact]
    public async Task BeginPass_on_a_full_pass_keeps_a_type_only_with_its_dependencies()
    {
        var store = await PublishedAsync("a", "b:a");

        Assert.Empty(store.BeginPass(2, Token2, []).DirtyTypes);

        Assert.Equal(["a", "b"], store.BeginPass(2, Token2, ["a"]).DirtyTypes);
    }

    [Fact]
    public async Task BeginPass_on_a_full_pass_keeps_a_dependency_without_its_dependent()
    {
        var store = await PublishedAsync("a", "b:a");

        Assert.Equal(["b"], store.BeginPass(2, Token2, ["b"]).DirtyTypes);
    }

    [Fact]
    public void The_first_pass_may_name_the_roots_alone_when_their_closure_is_every_type()
    {
        var store = Create("a", "b:a");

        Assert.Equal(["a", "b"], store.BeginPass(1, Token1, ["a"]).DirtyTypes);
        Assert.Throws<InvalidOperationException>(() => Create("a", "b:a", "c").BeginPass(1, Token1, ["a"]));
    }

    [Fact]
    public void BeginPass_still_refuses_a_type_no_reconciler_owns()
    {
        var store = Create("a", "b:a");

        Assert.Throws<ArgumentException>(() => store.BeginPass(1, Token1, ["ghost"]));
    }

    [Fact]
    public async Task A_store_without_a_graph_closes_nothing()
    {
        var store = Stores.Create("a", "b");
        await store.PublishAsync(1, Token1, ("a", "a1"), ("b", "b1"));

        Assert.Equal(["a"], store.BeginPass(2, Token2, ["a"]).DirtyTypes);
    }

    [Fact]
    public async Task A_graph_without_an_edge_closes_nothing()
    {
        var store = await PublishedAsync("a", "b");

        Assert.Equal(["a"], store.BeginPass(2, Token2, ["a"]).DirtyTypes);
    }

    [Fact]
    public async Task A_dependent_reads_its_dependencys_accepted_result_in_the_same_pass()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a2"));

        var states = pass.DependencyStates("b");

        Assert.Equal("a2", Assert.Single(states).Value);
        Assert.Equal("a", Assert.Single(states).Key);
    }

    [Fact]
    public async Task A_dependent_reads_the_base_state_of_a_dependency_outside_the_pass()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["b"]);

        Assert.Equal("a1", pass.DependencyStates("b")["a"]);
        Assert.Same(store.Current, pass.Base);
    }

    [Fact]
    public async Task A_dependent_reads_each_dependency_it_declares_and_no_other()
    {
        var store = await PublishedAsync("a", "b:a", "c:a", "d:b,c");
        var pass = store.BeginPass(2, Token2, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a2"));
        await pass.AcceptAsync("b", new ReconcileResult("b2"));
        await pass.AcceptAsync("c", new ReconcileResult("c2"));

        var states = pass.DependencyStates("d");

        Assert.Equal(["b", "c"], states.Keys.Order());
        Assert.Equal("b2", states["b"]);
        Assert.Equal("c2", states["c"]);
    }

    [Fact]
    public async Task DependencyStates_of_a_type_without_dependencies_is_empty()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);

        Assert.Empty(pass.DependencyStates("a"));
    }

    [Fact]
    public async Task DependencyStates_of_a_type_without_dependencies_is_empty_once_the_pass_ended()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);
        pass.Abandon();

        Assert.Empty(pass.DependencyStates("a"));
    }

    [Fact]
    public async Task DependencyStates_refuses_a_null_type()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);

        Assert.Throws<ArgumentNullException>("aggregateType", () => pass.DependencyStates(null!));
    }

    [Fact]
    public async Task DependencyStates_throws_for_a_dirty_dependency_not_yet_accepted()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);

        Assert.Throws<InvalidOperationException>(() => pass.DependencyStates("b"));
    }

    [Fact]
    public async Task DependencyStates_throws_once_the_pass_ended()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a2"));
        pass.Abandon();

        Assert.Throws<InvalidOperationException>(() => pass.DependencyStates("b"));
    }

    [Fact]
    public async Task DependencyStates_throws_once_the_pass_ended_although_its_dependency_is_clean()
    {
        var store = await PublishedAsync("a", "b:a");
        var pass = store.BeginPass(2, Token2, ["b"]);
        pass.Abandon();

        Assert.Throws<InvalidOperationException>(() => pass.DependencyStates("b"));
    }

    [Fact]
    public async Task HasDirtyDependency_is_true_for_a_type_with_a_dependency_in_the_pass()
    {
        var store = await PublishedAsync("a", "b:a", "c");

        var pass = store.BeginPass(2, Token2, ["a", "c"]);
        Assert.False(pass.HasDirtyDependency("a"));
        Assert.True(pass.HasDirtyDependency("b"));
        Assert.False(pass.HasDirtyDependency("c"));

        Assert.False(store.BeginPass(3, Token2, ["b"]).HasDirtyDependency("b"));
    }

    [Fact]
    public async Task Publishing_a_dependency_supersedes_its_dependents_state_in_the_same_swap()
    {
        var store = await PublishedAsync("a", "b:a", "c");
        var before = store.Current!;
        var pass = store.BeginPass(2, Token2, ["a"]);
        await pass.AcceptAsync("a", new ReconcileResult("a2"));
        await pass.AcceptAsync("b", new ReconcileResult("b2"));

        var after = store.Publish(pass)!;

        Assert.Equal("a2", after.Get("a"));
        Assert.Equal("b2", after.Get("b"));
        Assert.True(before.Superseded("a").IsCancellationRequested);
        Assert.True(before.Superseded("b").IsCancellationRequested);
        Assert.False(before.Superseded("c").IsCancellationRequested);
        Assert.False(before.TryActivate("b", () => { }));
        Assert.True(after.TryActivate("b", () => { }));
    }

    [Fact]
    public async Task A_pass_whose_dependent_fails_publishes_neither()
    {
        var store = await PublishedAsync("a", "b:a");
        var before = store.Current!;
        var pass = store.BeginPass(2, Token2, ["a"]);
        var accepted = await pass.RunAttemptAsync("a", _ => Task.FromResult(new ReconcileResult("a2")), Stores.Never);

        var failed = await pass.RunAttemptAsync("b", _ => throw new InvalidOperationException("b cannot read a2"), Stores.Never);

        Assert.Equal(AttemptOutcomeKind.Failed, failed.Kind);
        Assert.True(accepted.Attempt.Token.IsCancellationRequested);
        Assert.Null(store.Publish(pass));
        Assert.Same(before, store.Current);
        Assert.False(before.Superseded("a").IsCancellationRequested);
        Assert.False(before.Superseded("b").IsCancellationRequested);
    }

    private static ReconcileContext ContextOf(IReadOnlyDictionary<string, object> dependencies) =>
        new("b", 2, 1, 1, false, null, null, new FakeTransaction(), _ => throw new NotSupportedException(), dependencies);

    [Fact]
    public void GetDependency_returns_the_state_of_a_declared_dependency()
    {
        var context = ContextOf(new Dictionary<string, object> { ["a"] = "a2" });

        Assert.Equal("a2", context.GetDependency("a"));
        Assert.Equal("a2", context.GetDependency<string>("a"));
        Assert.Equal(["a"], context.Dependencies);
    }

    [Fact]
    public void GetDependency_of_an_undeclared_type_throws()
    {
        var context = ContextOf(new Dictionary<string, object> { ["a"] = "a2" });

        Assert.Throws<InvalidOperationException>(() => context.GetDependency("z"));
        Assert.Throws<InvalidOperationException>(() => context.GetDependency<string>("z"));
        Assert.Throws<ArgumentNullException>("aggregateType", () => context.GetDependency(null!));
    }

    [Fact]
    public void The_context_copies_the_dependencies_and_compares_their_names_ordinally()
    {
        var caller = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase) { ["a"] = "a2" };
        var context = ContextOf(caller);
        caller["z"] = "z2";

        Assert.Equal(["a"], context.Dependencies);
        Assert.Throws<InvalidOperationException>(() => context.GetDependency("A"));
        Assert.Throws<InvalidOperationException>(() => context.GetDependency("z"));
    }

    [Fact]
    public void The_constructor_without_dependencies_declares_none()
    {
        var context = new ReconcileContext("b", 2, 1, 1, false, null, null, new FakeTransaction(), _ => throw new NotSupportedException());

        Assert.Empty(context.Dependencies);
        Assert.Throws<InvalidOperationException>(() => context.GetDependency("a"));
    }

    [Fact]
    public void The_constructor_with_dependencies_refuses_null()
    {
        Assert.Throws<ArgumentNullException>(() => ContextOf(null!));
        Assert.Throws<ArgumentNullException>(() => ContextOf(new Dictionary<string, object> { ["a"] = null! }));
    }

    [Fact]
    public void The_constructor_refuses_a_null_type_transaction_or_snapshot_opener()
    {
        var none = new Dictionary<string, object>();

        Assert.Throws<ArgumentNullException>("aggregateType", () => new ReconcileContext(null!, 2, 1, 1, false, null, null, new FakeTransaction(), _ => throw new NotSupportedException(), none));
        Assert.Throws<ArgumentNullException>("transaction", () => new ReconcileContext("b", 2, 1, 1, false, null, null, null!, _ => throw new NotSupportedException(), none));
        Assert.Throws<ArgumentNullException>("openSnapshotSession", () => new ReconcileContext("b", 2, 1, 1, false, null, null, new FakeTransaction(), null!, none));
    }
}
