using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;

namespace Int.Log;

public sealed class ChangeLogMetricsTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private readonly List<ApplierRig> rigs = [];

    public ChangeLogMetricsTests(PostgresFixture postgres)
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
    public async Task Before_retention_deletes_the_poll_reports_the_head_a_horizon_of_0_and_every_entry_as_kept()
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a"]);
        rigs.Add(rig);
        await rig.LoadAsync();
        for (var i = 0; i < 4; i++)
        {
            await rig.WriteAsync(("a", $"k{i}"));
        }

        await rig.Applier.PollAsync(CancellationToken.None);

        var kept = await rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log");
        Assert.Equal(4L, kept);
        Assert.Contains(4L, Observe(ClusterDiagnostics.ChangeLogHead));
        Assert.Contains(0L, Observe(ClusterDiagnostics.ChangeLogHorizon));
        Assert.Contains(kept, Observe(ClusterDiagnostics.ChangeLogRetained));
    }

    [Fact]
    public async Task After_retention_the_poll_reports_the_horizon_it_raised_and_the_entries_it_kept()
    {
        const long Base = 9_100_000;
        var rig = await LoadedRigAsync(Base);
        for (var i = 0; i < 3; i++)
        {
            await rig.WriteAsync(("a", $"k{i}"));
        }

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        Assert.Equal(Base + 3, (await rig.RowCursorAsync()).Seq);
        await rig.WriteAsync(("a", "k3"));
        await rig.WriteAsync(("a", "k4"));

        Assert.Equal(new RetentionRun(Base + 3, 2), await Retention(rig).RunAsync());
        await rig.Applier.PollAsync(CancellationToken.None);

        var kept = await rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log");
        Assert.Equal(3L, kept);
        Assert.Contains(Base + 5, Observe(ClusterDiagnostics.ChangeLogHead));
        Assert.Contains(Base + 3, Observe(ClusterDiagnostics.ChangeLogHorizon));
        Assert.Contains(kept, Observe(ClusterDiagnostics.ChangeLogRetained));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());
        await rig.WriteAsync(("a", "k5"));
        Assert.Equal(new RetentionRun(Base + 5, 2), await Retention(rig).RunAsync());
        await rig.Applier.PollAsync(CancellationToken.None);

        kept = await rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log");
        Assert.Equal(2L, kept);
        Assert.Contains(Base + 6, Observe(ClusterDiagnostics.ChangeLogHead));
        Assert.Contains(Base + 5, Observe(ClusterDiagnostics.ChangeLogHorizon));
        Assert.Contains(kept, Observe(ClusterDiagnostics.ChangeLogRetained));
    }

    [Fact]
    public async Task A_pass_moves_the_head_before_the_next_poll()
    {
        const long Base = 9_200_000;
        var rig = await LoadedRigAsync(Base);
        await rig.WriteAsync(("a", "k1"));
        Assert.DoesNotContain(Base + 1, Observe(ClusterDiagnostics.ChangeLogHead));

        Assert.Equal(PassOutcome.Published, await rig.PassAsync());

        Assert.Contains(Base + 1, Observe(ClusterDiagnostics.ChangeLogHead));
    }

    private static Retention Retention(ApplierRig rig) => new(
        rig.Data, Options.Create(rig.Options), TimeProvider.System, NoFaultPoints.Instance, rig.Events, NullLogger<Retention>.Instance);

    private async Task<ApplierRig> LoadedRigAsync(long baseSeq)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a"]);
        rigs.Add(rig);
        await rig.LoadAsync();
        await rig.ExecuteAsync($"SELECT setval('reeve_cluster.change_log_seq', {baseSeq})");
        return rig;
    }

    private static List<long> Observe(Instrument instrument)
    {
        var values = new List<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (published, l) =>
        {
            if (ReferenceEquals(published, instrument))
            {
                l.EnableMeasurementEvents(published);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => values.Add(value));
        listener.Start();
        listener.RecordObservableInstruments();
        return values;
    }
}
