using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Log;

internal sealed class AckRig : IAsyncDisposable
{
    private AckRig(SkipRig skip, TimeSpan ackTimeout, TimeSpan pollInterval)
    {
        Skip = skip;
        Channel = new InProcessWakeupChannel(NullLogger<InProcessWakeupChannel>.Instance);
        Data = new ClusterDataSource(new ClusterOptions { ConnectionString = skip.ConnectionString });
        var options = Options.Create(new ClusterOptions
        {
            ConnectionString = skip.ConnectionString,
            ReplicaId = "r1",
            AckTimeout = ackTimeout,
            AckPollInterval = pollInterval,
        });
        Ack = new ChangeAck(
            Log.Scopes, Log.Refusal, Log.Gate, Local, Data, skip.Recorder, Channel, options, skip.Time, skip.Faults, skip.Events,
            NullLogger<ChangeAck>.Instance);
    }

    public SkipRig Skip { get; }

    public TimeProvider Time => Skip.Time;

    public LogRig Log => Skip.Log;

    public InProcessWakeupChannel Channel { get; }

    public LocalGeneration Local { get; } = new();

    public ClusterDataSource Data { get; }

    public ChangeAck Ack { get; }

    public static async Task<AckRig> CreateAsync(
        PostgresFixture postgres, TimeSpan? ackTimeout = null, TimeSpan? pollInterval = null, Func<string, string>? connectionString = null,
        TimeProvider? time = null) =>
        new(await SkipRig.CreateAsync(postgres, time: time, connectionString: connectionString), ackTimeout ?? TimeSpan.FromSeconds(20), pollInterval ?? TimeSpan.FromMilliseconds(100));

    public Task<long> WriteAsync(string type = "widget") => Skip.WriteAsync(type);

    public async Task LocalAtAsync(long seq) => Local.Current = new Gen(seq, await TokenAtAsync(seq));

    public async Task MemberAsync(string id, string state = "active", long? activatedAtSeq = 0, TimeSpan? leaseFor = null)
    {
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO reeve_cluster.member (replica_id, epoch, state, lease_expires_at, activated_at_seq, version, protocol, process_id)
            VALUES ($1, 1, $2, clock_timestamp() + make_interval(secs => $3), $4, '1.0.0', 1, gen_random_uuid())
            """, connection)
        {
            Parameters =
            {
                new NpgsqlParameter<string> { TypedValue = id },
                new NpgsqlParameter<string> { TypedValue = state },
                new NpgsqlParameter<double> { TypedValue = (leaseFor ?? TimeSpan.FromMinutes(5)).TotalSeconds },
                new NpgsqlParameter { Value = (object?)activatedAtSeq ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint },
            },
        };
        await command.ExecuteNonQueryAsync();
    }

    public async Task ApplyAsync(string id, long seq, Guid? token = null, bool notice = true)
    {
        await using (var connection = await Log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                "UPDATE reeve_cluster.member SET applied_seq = $2, applied_token = $3 WHERE replica_id = $1", connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string> { TypedValue = id },
                    new NpgsqlParameter<long> { TypedValue = seq },
                    new NpgsqlParameter<Guid> { TypedValue = token ?? await TokenAtAsync(seq) },
                },
            };
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        if (notice)
        {
            await Channel.PublishAsync(new Wakeup(WakeupKind.Applied, seq));
        }
    }

    public async Task<Guid> TokenAtAsync(long seq) =>
        await Log.ScalarAsync<Guid>($"SELECT entry_token FROM reeve_cluster.change_log WHERE seq = {seq}");

    public Task<long> AckedSeqAsync() => Skip.AckedSeqAsync();

    public IReadOnlyList<ModelEvent> EventsOf(string action) => Skip.EventsOf(action);

    public async ValueTask DisposeAsync()
    {
        Ack.Dispose();
        await Data.DisposeAsync();
        await Skip.DisposeAsync();
    }

    internal sealed class LocalGeneration : IGenerationSource
    {
        public IGeneration? Current { get; set; }

        public void EnsureNotInsideTheSwap() => throw new NotSupportedException();
    }

    private sealed class Gen(long head, Guid token) : IGeneration
    {
        public long Head => head;

        public Guid? HeadToken => token;

        public object Get(string aggregateType) => throw new NotSupportedException();

        public CancellationToken Superseded(string aggregateType) => CancellationToken.None;

        public bool TryActivate(string aggregateType, Action activate) => throw new NotSupportedException();
    }
}
