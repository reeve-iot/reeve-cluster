using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Unit.Claims;

public sealed class PrimitiveRetentionTests
{
    private readonly FakeTimeProvider time = new();
    private readonly ScriptedRetentionRows rows = new();

    private PrimitiveRetention Retention(int limit = 7) => new(
        rows, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5), RetentionInterval = TimeSpan.FromSeconds(60) }), time,
        NullLogger<PrimitiveRetention>.Instance, limit);

    [Fact]
    public async Task A_pass_runs_every_half_with_the_limit_and_totals_the_deleted_rows()
    {
        rows.Claims = 1;
        rows.Once = 2;
        rows.Flights = 3;
        rows.Budgets = new BudgetRetained(4, 40);
        rows.Counters = 5;
        rows.Slots = 6;

        var deleted = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(1 + 2 + 3 + 4 + 5 + 6, deleted);
        Assert.Equal(["claims", "once", "flights", "counters", "slots", "budgets"], rows.Calls.Select(c => c.Half));
        Assert.All(rows.Calls, c => Assert.Equal(7, c.Limit));
    }

    [Fact]
    public async Task A_full_batch_is_repeated_until_a_short_one()
    {
        rows.Script("claims", 7, 7, 3);
        rows.Script("budgets", 7);
        rows.Budgets = new BudgetRetained(0, 0);

        var deleted = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(7 + 7 + 3 + 7, deleted);
        Assert.Equal(3, rows.Calls.Count(c => c.Half == "claims"));
        Assert.Equal(1, rows.Calls.Count(c => c.Half == "once"));
        Assert.Equal(2, rows.Calls.Count(c => c.Half == "budgets"));
    }

    [Fact]
    public async Task A_short_batch_ends_the_repeat_while_the_budget_is_unused()
    {
        rows.Script("claims", 3);

        var deleted = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(3, deleted);
        Assert.Equal(1, rows.Calls.Count(c => c.Half == "claims"));
    }

    [Fact]
    public async Task A_budget_half_is_full_when_the_windows_or_the_ids_fill_their_batch()
    {
        rows.Script("budgets", 0, 0);
        rows.Budgets = new BudgetRetained(0, 7 * RetentionStore.IdsPerWindow);
        rows.AfterCall = () =>
        {
            if (rows.Calls.Any(c => c.Half == "budgets"))
            {
                rows.Budgets = new BudgetRetained(0, 2);
            }
        };

        _ = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(2, rows.Calls.Count(c => c.Half == "budgets"));
    }

    [Fact]
    public async Task A_half_stops_repeating_when_its_budget_is_used_but_always_runs_once()
    {
        rows.Script("claims", 7, 7, 7, 7, 7, 7, 7, 7, 7, 7);
        rows.AfterCall = () => time.Advance(TimeSpan.FromSeconds(2));

        _ = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(3, rows.Calls.Count(c => c.Half == "claims"));
        Assert.Equal(1, rows.Calls.Count(c => c.Half == "once"));
    }

    [Fact]
    public async Task A_run_the_library_s_bound_cancelled_ends_the_repeat_and_is_not_counted()
    {
        rows.Script("claims", 7, 7);
        rows.HangOnCall = ("claims", 2);
        var pass = Retention().PassAsync(CancellationToken.None);

        while (rows.Calls.Count(c => c.Half == "claims") < 2)
        {
            await Task.Yield();
        }

        time.Advance(TimeSpan.FromSeconds(8));

        Assert.Equal(7, await pass);
        Assert.Equal(2, rows.Calls.Count(c => c.Half == "claims"));
    }

    [Fact]
    public async Task A_half_that_fails_after_a_full_batch_keeps_what_it_deleted_and_the_others_go_on()
    {
        rows.Script("claims", 7);
        rows.FailOnCall = ("claims", 2);
        rows.Slots = 2;

        var deleted = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(7 + 2, deleted);
    }

    [Fact]
    public async Task A_half_that_fails_is_logged_and_the_others_go_on()
    {
        rows.FailOnce = "once";
        rows.Claims = 1;
        rows.Slots = 2;

        var deleted = await Retention().PassAsync(CancellationToken.None);

        Assert.Equal(3, deleted);
        Assert.Contains("budgets", rows.Calls.Select(c => c.Half));
    }

    [Fact]
    public async Task A_half_the_library_s_bound_cancelled_is_not_counted_and_does_not_stop_the_pass()
    {
        rows.Claims = 5;
        rows.HangOn = "claims";
        rows.Once = 2;
        var pass = Retention().PassAsync(CancellationToken.None);

        while (!rows.Calls.Any(c => c.Half == "claims"))
        {
            await Task.Yield();
        }

        time.Advance(TimeSpan.FromSeconds(8));

        Assert.Equal(2, await pass);
    }

    [Fact]
    public async Task A_stop_ends_the_loop_after_the_pass_in_hand()
    {
        using var stop = new CancellationTokenSource();
        var unit = Retention();
        var loop = unit.OnAcquiredAsync(null!, stop.Token);
        while (rows.Calls.Count < 6)
        {
            await Task.Yield();
        }

        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => loop);
    }

    [Fact]
    public void The_unit_is_registered_once_as_a_convergent_unit_next_to_the_once_sweeper()
    {
        var services = new ServiceCollection();
        services.AddClusterClaims();
        services.AddClusterClaims();

        var kinds = services.Select(d => d.ImplementationInstance).OfType<UnitKind>().Where(k => k.Kind == PrimitiveRetention.Kind).ToList();

        var kind = Assert.Single(kinds);
        Assert.True(kind.Options.Convergent);
        Assert.Contains(services, d => d.ImplementationInstance is UnitKind { Kind: OnceSweeper.Kind });
    }

    [Fact]
    public void The_defaults_delete_no_once_record_and_no_counter()
    {
        var options = new ClusterOptions();

        Assert.Null(options.OnceRetention);
        Assert.Null(options.CounterRetention);
    }

    [Theory]
    [InlineData("OnceRetention", 0)]
    [InlineData("OnceRetention", -1)]
    [InlineData("CounterRetention", 0)]
    [InlineData("CounterRetention", -1)]
    public void A_retention_that_is_not_positive_is_refused(string setting, int seconds)
    {
        var options = Valid();
        typeof(ClusterOptions).GetProperty(setting)!.SetValue(options, TimeSpan.FromSeconds(seconds));

        var problem = Assert.Single(OptionsValidator.Validate(options, aspNetCoreInUse: false));

        Assert.Equal(OptionsViolation.NotPositive, problem.Rule);
        Assert.Equal(setting, problem.Setting);
    }

    [Theory]
    [InlineData("OnceRetention")]
    [InlineData("CounterRetention")]
    public void A_retention_beyond_the_command_age_limit_is_refused_and_one_on_the_limit_is_accepted(string setting)
    {
        var property = typeof(ClusterOptions).GetProperty(setting)!;
        var onTheLimit = Valid();
        property.SetValue(onTheLimit, OptionsValidator.MaxCommandAge);
        var over = Valid();
        property.SetValue(over, OptionsValidator.MaxCommandAge + TimeSpan.FromTicks(1));

        Assert.Empty(OptionsValidator.Validate(onTheLimit, aspNetCoreInUse: false));
        var problem = Assert.Single(OptionsValidator.Validate(over, aspNetCoreInUse: false));
        Assert.Equal(OptionsViolation.DurationTooLong, problem.Rule);
        Assert.Equal(setting, problem.Setting);
    }

    private static ClusterOptions Valid() => new()
    {
        ReplicaId = "replica-1",
        ConnectionString = "Host=db;Database=app;Username=u",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
    };

    private sealed class ScriptedRetentionRows : IRetentionRows
    {
        private const int RunawayCalls = 100;

        private readonly Dictionary<string, Queue<int>> scripts = [];

        public List<(string Half, int Limit)> Calls { get; } = [];

        public int Claims { get; set; }

        public int Once { get; set; }

        public int Flights { get; set; }

        public BudgetRetained Budgets { get; set; }

        public int Counters { get; set; }

        public int Slots { get; set; }

        public string? FailOnce { get; set; }

        public string? HangOn { get; set; }

        public (string Half, int Call) HangOnCall { get; set; }

        public (string Half, int Call) FailOnCall { get; set; }

        public Action? AfterCall { get; set; }

        public void Script(string half, params int[] counts) => scripts[half] = new Queue<int>(counts);

        public Task<int> DeleteClaimsAsync(int limit, CancellationToken cancellationToken) => Answer("claims", limit, Claims, cancellationToken);

        public Task<int> DeleteOnceAsync(int limit, CancellationToken cancellationToken) => Answer("once", limit, Once, cancellationToken);

        public Task<int> DeleteFlightsAsync(int limit, CancellationToken cancellationToken) => Answer("flights", limit, Flights, cancellationToken);

        public async Task<BudgetRetained> DeleteBudgetsAsync(int limit, CancellationToken cancellationToken)
        {
            var ids = Budgets.Ids;
            return new(await Answer("budgets", limit, Budgets.Windows, cancellationToken), ids);
        }

        public Task<int> DeleteCountersAsync(int limit, CancellationToken cancellationToken) => Answer("counters", limit, Counters, cancellationToken);

        public Task<int> DeleteSlotsAsync(int limit, CancellationToken cancellationToken) => Answer("slots", limit, Slots, cancellationToken);

        private async Task<int> Answer(string half, int limit, int count, CancellationToken cancellationToken)
        {
            Calls.Add((half, limit));

            if (Calls.Count > RunawayCalls)
            {
                throw new InvalidOperationException("scripted: a repeat that does not stop");
            }

            var call = Calls.Count(c => c.Half == half);
            if (scripts.TryGetValue(half, out var script) && script.Count > 0)
            {
                count = script.Dequeue();
            }

            if (FailOnCall == (half, call))
            {
                throw new InvalidOperationException("scripted");
            }

            if (HangOnCall == (half, call))
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            AfterCall?.Invoke();
            if (FailOnce == half)
            {
                FailOnce = null;
                throw new InvalidOperationException("scripted");
            }

            if (HangOn == half)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            return count;
        }
    }
}
