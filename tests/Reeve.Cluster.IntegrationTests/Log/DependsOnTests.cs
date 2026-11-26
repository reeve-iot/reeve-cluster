using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;

namespace Int.Log;

public sealed class DependsOnTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly IReadOnlyDictionary<string, string[]> BOnA = new Dictionary<string, string[]> { ["b"] = ["a"] };

    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public DependsOnTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var rig in rigs)
        {
            await rig.DisposeAsync();
        }
    }

    [Fact]
    public async Task Row1_an_entry_of_the_dependency_reconciles_its_clean_dependent_without_hints_and_publishes_both_in_one_swap()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var before = rig.Store.Current!;
        await rig.WriteAsync(("a", "k1"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)LastPassBegin(rig)["dirty"]!);
        Assert.Equal(1L, LastPassBegin(rig)["entries"]);
        Assert.Equal(new HashSet<string> { "k1" }, rig.State("a").Hints);
        Assert.Null(rig.State("b").Hints);
        Assert.Same(rig.State("a"), rig.State("b").Dependencies["a"]);
        Assert.True(before.Superseded("a").IsCancellationRequested);
        Assert.True(before.Superseded("b").IsCancellationRequested);
    }

    [Fact]
    public async Task Row2_an_entry_of_the_dependent_alone_reconciles_it_with_its_hints_against_the_base_state_of_the_dependency()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var a = rig.State("a");
        await rig.WriteAsync(("b", "k2"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Single(rig["a"].Calls);
        Assert.Same(a, rig.State("a"));
        Assert.Equal(new HashSet<string> { "k2" }, rig.State("b").Hints);
        Assert.Same(a, rig.State("b").Dependencies["a"]);
    }

    [Fact]
    public async Task Row3_a_dependent_dirty_by_itself_too_runs_without_hints_after_its_dependency()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        await rig.WriteAsync(("a", "k1"), ("b", "k2"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(new HashSet<string> { "k1" }, rig.State("a").Hints);
        Assert.Null(rig.State("b").Hints);
        Assert.Same(rig.State("a"), rig.State("b").Dependencies["a"]);
    }

    [Fact]
    public async Task Row9_a_dependency_whose_every_entry_a_skip_covers_stays_out_and_the_dependent_reads_its_base_state()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var a = rig.State("a");
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("b", "k2"));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Single(rig["a"].Calls);
        Assert.Same(a, rig.State("a"));
        Assert.Equal(new HashSet<string> { "k2" }, rig.State("b").Hints);
        Assert.Same(a, rig.State("b").Dependencies["a"]);
    }

    [Fact]
    public async Task Row14a_a_reload_keeps_a_dependent_and_its_dependency_when_both_qualify()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        await OpenAsync(rig, "a", await rig.WriteAsync(("a", "k1")));
        await OpenAsync(rig, "b", await rig.WriteAsync(("b", "k2")));

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.Single(rig["a"].Calls);
        Assert.Single(rig["b"].Calls);
        var begin = LastPassBegin(rig);
        Assert.Empty((IEnumerable<string>)begin["dirty"]!);
        Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)begin["kept"]!);
    }

    [Fact]
    public async Task Row14b_a_reload_keeps_a_qualifying_dependency_and_reloads_the_dependent_against_it()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var a = rig.State("a");
        var from = await rig.WriteAsync(("a", "k1"));
        await OpenAsync(rig, "a", from);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.Single(rig["a"].Calls);
        Assert.True(rig["b"].Calls.Last().FullLoad);
        Assert.Same(a, rig.State("b").Dependencies["a"]);
        var begin = LastPassBegin(rig);
        Assert.Equal(new[] { "b" }, (IEnumerable<string>)begin["dirty"]!);
        Assert.Equal(new[] { "a" }, (IEnumerable<string>)begin["kept"]!);
    }

    [Fact]
    public async Task Row14c_a_reload_does_not_keep_a_qualifying_dependent_over_a_dependency_that_reloads()
    {
        var logger = new ListLogger();
        var rig = await RigAsync(logger);
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("b", "k2"));
        await OpenAsync(rig, "b", from);

        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));

        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.Equal(2, rig["b"].Calls.Count);
        Assert.Same(rig.State("a"), rig.State("b").Dependencies["a"]);
        var begin = LastPassBegin(rig);
        Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)begin["dirty"]!);
        Assert.Empty((IEnumerable<string>)begin["kept"]!);
        Assert.Single(logger.Messages, m => m.Contains("The reload reconciles b although", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Row15_the_repair_entry_of_a_dependencys_skip_reconciles_the_dependency_and_its_dependent_without_hints()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var from = await rig.WriteAsync(("a", "k1"));
        var skip = await OpenAsync(rig, "a", from);
        await rig.WriteAsync(("a", "k2"));
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Single(rig["a"].Calls);
        var loadedA = rig.State("a");
        var repair = await rig.WriteAsync(("a", null));
        await CloseAsync(rig, skip, repair);

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Equal(2, rig["a"].Calls.Count);
        Assert.Equal(2, rig["b"].Calls.Count);
        Assert.Null(rig.State("a").Hints);
        Assert.Null(rig.State("b").Hints);
        Assert.NotSame(loadedA, rig.State("a"));
        Assert.Same(rig.State("a"), rig.State("b").Dependencies["a"]);
    }

    [Fact]
    public async Task Row16a_a_skip_of_the_dependent_passes_its_poison_while_the_dependency_has_no_uncovered_entry()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var poison = await rig.WriteAsync(("b", "poison"));
        await OpenAsync(rig, "b", poison);
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Single(rig["b"].Calls);
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["dirty"]!);
    }

    [Fact]
    public async Task Row16b_a_skip_of_the_dependent_alone_stalls_while_the_dependency_has_an_uncovered_entry_and_the_warning_names_the_skips()
    {
        var logger = new ListLogger();
        var rig = await RigAsync(logger);
        await rig.LoadAsync();
        var loaded = rig.Store.Current;
        await rig.WriteAsync(("a", "k1"));
        var poison = await rig.WriteAsync(("b", "poison"));
        await OpenAsync(rig, "b", poison);
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");

        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync());

        Assert.Same(loaded, rig.Store.Current);
        Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)LastPassBegin(rig)["dirty"]!);
        var warning = Assert.Single(logger.Messages, m => m.Contains("The reconciler of b failed", StringComparison.Ordinal));
        Assert.Contains("a, b", warning, StringComparison.Ordinal);
        Assert.Contains("fromSeq = ackedSeq + 1", warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Row17_skips_of_the_dependency_and_the_dependent_from_the_acknowledged_seq_pass_a_poison_write_that_starts_with_the_dependency()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var loaded = rig.Store.Current;

        var poison = await rig.WriteAsync(("a", "ka"), ("b", "kb"));
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");
        await OpenAsync(rig, "a", poison);
        await OpenAsync(rig, "b", poison);

        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync());
        Assert.Same(loaded, rig.Store.Current);

        await OpenAsync(rig, "a", 1);
        await OpenAsync(rig, "b", 1);
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Empty((IEnumerable<string>)LastPassBegin(rig)["dirty"]!);
        Assert.Equal((await rig.HeadAsync()).Seq, rig.Store.Current!.Head);
        Assert.Same(loaded!.Get("a"), rig.Store.Current.Get("a"));
        Assert.Same(loaded.Get("b"), rig.Store.Current.Get("b"));
    }

    [Fact]
    public async Task Row17_on_a_rejoin_the_skips_from_the_poisons_seq_reload_both_and_stall_and_those_from_the_acknowledged_seq_keep_both()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var poison = await rig.WriteAsync(("a", "ka"), ("b", "kb"));
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");
        await OpenAsync(rig, "a", poison);
        await OpenAsync(rig, "b", poison);

        Assert.Equal(PassOutcome.Stalled, await ReloadAsync(rig));

        await OpenAsync(rig, "a", 1);
        await OpenAsync(rig, "b", 1);
        Assert.Equal(PassOutcome.Published, await ReloadAsync(rig));
        var begin = LastPassBegin(rig);
        Assert.Empty((IEnumerable<string>)begin["dirty"]!);
        Assert.Equal(new[] { "a", "b" }, (IEnumerable<string>)begin["kept"]!);
    }

    [Fact]
    public async Task A_poison_write_with_two_keys_of_one_type_skipped_from_the_second_key_stalls_on_the_retry_without_hints()
    {
        var rig = await RigAsync(dependent: false);
        await rig.LoadAsync();
        var second = await rig.WriteAsync(("b", "k1"), ("b", "k2"));
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");
        await OpenAsync(rig, "b", second);

        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync());
        Assert.Equal(new HashSet<string> { "k1" }, rig["b"].Calls.Last().Hints);
        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync(withoutHints: true));
        Assert.Null(rig["b"].Calls.Last().Hints);

        await OpenAsync(rig, "b", 1);
        Assert.Equal(PassOutcome.Published, await rig.PassAsync(withoutHints: true));
    }

    [Fact]
    public async Task Row23_closing_the_dependencys_skip_before_the_dependents_fix_runs_stalls_again()
    {
        var rig = await RigAsync();
        await rig.LoadAsync();
        var poison = await rig.WriteAsync(("a", "ka"), ("b", "kb"));
        rig["b"].Before = (_, _) => throw new InvalidOperationException("poison");
        var skipOfA = await OpenAsync(rig, "a", 1);
        await OpenAsync(rig, "b", 1);
        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        var repair = await rig.WriteAsync(("a", null));
        await CloseAsync(rig, skipOfA, repair);

        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync());
        Assert.True(repair > poison);
    }

    [Fact]
    public async Task A_dependent_reads_an_undeclared_type_only_by_stalling()
    {
        var rig = await RigAsync(dependent: false);
        await rig.LoadAsync();
        rig["b"].Before = (context, _) => Task.FromResult(context.GetDependency("a"));
        await rig.WriteAsync(("b", "k2"));

        Assert.Equal(PassOutcome.Stalled, await rig.PassAsync());
    }

    private static Task<PassOutcome> ReloadAsync(ApplierRig rig) =>
        rig.Applier.RunPassAsync(rig.Epoch, forceFull: true, withoutHints: false, CancellationToken.None);

    private static IReadOnlyDictionary<string, object?> LastPassBegin(ApplierRig rig) =>
        rig.Events.All.Last(e => e.Action == LogActions.PassBegin).State!;

    private static async Task<long> OpenAsync(ApplierRig rig, string type, long from)
    {
        await using var connection = await rig.Log.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            $"INSERT INTO reeve_cluster.change_skip (aggregate_type, from_seq, reason, opened_by) VALUES ('{type}', {from}, 'test', 'operator') RETURNING id",
            connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static Task CloseAsync(ApplierRig rig, long skipId, long until) =>
        rig.ExecuteAsync(
            $"UPDATE reeve_cluster.change_skip SET closed_by = 'operator', closed_at = clock_timestamp(), until_seq = {until} WHERE id = {skipId}");

    private async Task<ApplierRig> RigAsync(ILogger<Applier>? logger = null, bool dependent = true)
    {
        var rig = await ApplierRig.CreateAsync(
            postgres, ["a", "b"], applierLogger: logger, dependsOn: dependent ? BOnA : new Dictionary<string, string[]>());
        rigs.Add(rig);
        return rig;
    }

    private sealed class ListLogger : ILogger<Applier>
    {
        private readonly ConcurrentQueue<string> messages = new();

        public IReadOnlyList<string> Messages => [.. messages];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            messages.Enqueue(formatter(state, exception));
    }
}
