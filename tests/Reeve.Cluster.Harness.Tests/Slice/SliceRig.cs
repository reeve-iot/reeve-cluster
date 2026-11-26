using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;

namespace Harness.Slice;

internal sealed class SliceRig : IAsyncDisposable
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);
    private readonly Dictionary<Replica, HttpClient> clients = [];

    private SliceRig(HarnessRun run) => Run = run;

    public HarnessRun Run { get; }

    public static async Task<SliceRig> CreateAsync(StackFixture stack, Action<string> log)
    {
        var run = await HarnessRun.BeginAsync(log, stack.Stack);
        try
        {
            await run.MigrateAsync();
            await using var connection = await run.OpenObserverConnectionAsync();
            await using var command = new NpgsqlCommand(SliceApp.CreateTable, connection);
            await command.ExecuteNonQueryAsync();
            return new SliceRig(run);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public Task<Replica> PrepareAsync(string id, ReplicaSpec? spec = null) => Run.PrepareAsync(id, spec ?? SliceApp.Spec());

    public async Task<Replica> StartAsync(string id, ReplicaSpec? spec = null)
    {
        var replica = await PrepareAsync(id, spec);
        await replica.StartAsync();
        await replica.Started!.WaitAsync(Limit);
        return replica;
    }

    public async Task<(Replica R1, Replica R2)> StartPairAsync(ReplicaSpec? spec = null)
    {
        spec ??= SliceApp.Spec();
        var r1 = await StartAsync("r1", spec);
        await WaitForPhaseAsync(r1, ReplicaPhase.Serving);
        var r2 = await StartAsync("r2", spec);
        await WaitForPhaseAsync(r2, ReplicaPhase.Serving);
        return (r1, r2);
    }

    public HttpClient Client(Replica replica)
    {
        lock (clients)
        {
            if (!clients.TryGetValue(replica, out var client))
            {
                clients[replica] = client = new HttpClient { BaseAddress = replica.Url, Timeout = TimeSpan.FromSeconds(90) };
            }

            return client;
        }
    }

    public Task<RawHub> ConnectHubAsync(Replica replica) => RawHub.ConnectAsync(new Uri(replica.Url, "hub"));

    public static ReplicaPhase PhaseOf(Replica replica) => replica.Services.GetRequiredService<IPhaseGate>().Phase;

    public static async Task WaitForPhaseAsync(Replica replica, ReplicaPhase phase, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? Limit);
        while (PhaseOf(replica) != phase)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"{replica.Id} is {PhaseOf(replica)}, not {phase}.");
            }

            await Task.Delay(20);
        }
    }

    public static async Task UntilAsync(Func<Task<bool>> condition, string what, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? Limit);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }

            await Task.Delay(20);
        }
    }

    public async Task WaitForLockWaitAsync(string application, Task task)
    {
        var deadline = DateTime.UtcNow + Limit;
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1 AND wait_event_type = 'Lock'",
            connection)
        {
            Parameters = { new NpgsqlParameter<string> { TypedValue = application } },
        };
        while (!task.IsCompleted && (long)(await command.ExecuteScalarAsync())! == 0)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"{application} neither ended nor waited for a lock.");
            }

            await Task.WhenAny(task, Task.Delay(5));
        }
    }

    public static async Task<string?> ErrorCodeAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadAsStringAsync();
        if (body.Length == 0)
        {
            return null;
        }

        using var document = JsonDocument.Parse(body);
        return document.RootElement.TryGetProperty("errorCode", out var code) ? code.GetString() : null;
    }

    public static async Task<ItemView> ItemAsync(HttpResponseMessage response) =>
        JsonSerializer.Deserialize<ItemView>(await response.Content.ReadAsStringAsync(), new JsonSerializerOptions(JsonSerializerDefaults.Web))!;

    public async ValueTask DisposeAsync()
    {
        foreach (var client in clients.Values)
        {
            client.Dispose();
        }

        await Run.DisposeAsync();
    }
}
