using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Claims;

namespace Unit.Claims;

public sealed class ClusterCounterTests
{
    private readonly FakeTimeProvider time = new();
    private readonly ScriptedCounterRows rows = new();

    private ClusterCounter Counter() =>
        new(rows, Options.Create(new ClusterOptions { FencedTransactionTimeout = TimeSpan.FromSeconds(5) }), time, NullLogger<ClusterCounter>.Instance);

    [Fact]
    public async Task An_increment_answers_the_value_after_it()
    {
        rows.Values.Enqueue(7);

        var result = await Counter().IncrementAsync("k");

        Assert.Equal(new CounterResult.Incremented(7), result);
        Assert.Equal(["k"], rows.Calls);
    }

    [Fact]
    public async Task I33_an_increment_the_librarys_own_bound_cancelled_is_unknown_although_the_statement_returned_a_value()
    {
        rows.OnIncrement = () => time.Advance(TimeSpan.FromSeconds(30));
        rows.Values.Enqueue(1);

        Assert.Equal(new CounterResult.Unknown(), await Counter().IncrementAsync("k"));
    }

    [Fact]
    public async Task I33_an_increment_the_caller_cancelled_before_it_returned_throws()
    {
        using var cancel = new CancellationTokenSource();
        rows.OnIncrement = () => cancel.Cancel();
        rows.Values.Enqueue(1);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Counter().IncrementAsync("k", cancel.Token));
    }

    [Fact]
    public async Task A_database_failure_is_thrown()
    {
        rows.Failure = new DbFailure("the database is down");

        await Assert.ThrowsAsync<DbFailure>(() => Counter().IncrementAsync("k"));
    }

    [Fact]
    public async Task An_increment_needs_a_key()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Counter().IncrementAsync(null!));
        Assert.Empty(rows.Calls);
    }

    private sealed class ScriptedCounterRows : ICounterRows
    {
        public Queue<long> Values { get; } = new();

        public List<string> Calls { get; } = [];

        public Action? OnIncrement { get; set; }

        public Exception? Failure { get; set; }

        public Task<long> IncrementAsync(string key, CancellationToken cancellationToken)
        {
            Calls.Add(key);
            OnIncrement?.Invoke();
            return Failure is null ? Task.FromResult(Values.Dequeue()) : Task.FromException<long>(Failure);
        }
    }
}
