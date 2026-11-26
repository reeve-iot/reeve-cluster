using Npgsql;
using System.IO;
using Reeve.Cluster;
using Reeve.Cluster.Log;
using Reeve.Cluster.Owned;
using Xunit;

namespace Unit.Diagnostics;

[Collection(MetricsCollection.Name)]
public sealed class CommitMetricsTests
{
    [Fact]
    public async Task Library_commit_records_committed_unknown_and_failed()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.CommitDuration);
        await TransactionTimeout.CommitAsync(_ => Task.CompletedTask, CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TransactionTimeout.CommitAsync(_ =>
        {
            cancellation.Cancel();
            return Task.CompletedTask;
        }, cancellation.Token));
        await Assert.ThrowsAsync<InvalidOperationException>(() => TransactionTimeout.CommitAsync(_ => Task.FromException(new InvalidOperationException()), CancellationToken.None));

        var results = recorder.Measurements();
        Assert.Collection(results,
            m => Assert.Equal(("library", "committed"), ((string)m.Tags["transaction"]!, (string)m.Tags["outcome"]!)),
            m => Assert.Equal(("library", "unknown"), ((string)m.Tags["transaction"]!, (string)m.Tags["outcome"]!)),
            m => Assert.Equal(("library", "failed"), ((string)m.Tags["transaction"]!, (string)m.Tags["outcome"]!)));
        Assert.All(results, m => Assert.True(m.Value >= 0));
    }

    [Fact]
    public async Task Library_commit_records_unknown_when_the_connection_is_lost()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.CommitDuration);
        await using var connection = new NpgsqlConnection();
        await Assert.ThrowsAsync<NpgsqlException>(() => TransactionTimeout.CommitAsync(
            _ => Task.FromException(new NpgsqlException("lost", new IOException("transport closed"))),
            CancellationToken.None, connection));
        var measurement = Assert.Single(recorder.Measurements());
        Assert.Equal("library", measurement.Tags["transaction"]);
        Assert.Equal("unknown", measurement.Tags["outcome"]);
    }

    [Fact]
    public async Task Fenced_commit_records_committed_and_unknown_on_closed_connection()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.CommitDuration);
        await using var connection = new NpgsqlConnection();
        await FencedTransactions.CommitAsync(_ => Task.CompletedTask, connection, CancellationToken.None);
        await Assert.ThrowsAsync<CommitOutcomeUnknownException>(() => FencedTransactions.CommitAsync(
            _ => Task.FromException(new InvalidOperationException("transport interrupted")), connection, CancellationToken.None));
        var results = recorder.Measurements();
        Assert.Equal(2, results.Count);
        Assert.Equal(("fenced", "committed"), ((string)results[0].Tags["transaction"]!, (string)results[0].Tags["outcome"]!));
        Assert.Equal(("fenced", "unknown"), ((string)results[1].Tags["transaction"]!, (string)results[1].Tags["outcome"]!));
    }

    [Fact]
    public async Task Fenced_commit_records_a_server_error_as_failed()
    {
        using var recorder = new InstrumentRecorder(ClusterDiagnostics.CommitDuration);
        await using var connection = new NpgsqlConnection();
        var serverError = new PostgresException("server rejected commit", "ERROR", "ERROR", "XX000");
        await Assert.ThrowsAsync<PostgresException>(() => FencedTransactions.CommitAsync(
            _ => Task.FromException(serverError), connection, CancellationToken.None));
        var measurement = Assert.Single(recorder.Measurements());
        Assert.Equal("fenced", measurement.Tags["transaction"]);
        Assert.Equal("failed", measurement.Tags["outcome"]);
    }
}
