using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Reeve.Cluster.Harness.Linearizability;
using Reeve.Cluster.Harness.Seeds;

namespace Harness.Linearizability;

public sealed class RecorderTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromMinutes(1);

    [Theory]
    [InlineData(200, null, false, "Ok")]
    [InlineData(202, "write_unconfirmed", true, "Unknown")]
    [InlineData(200, null, true, "Unknown")]
    [InlineData(500, null, true, "Unknown")]
    [InlineData(503, "replica_fenced", false, "Failed")]
    [InlineData(503, "replica_not_serving", false, "Failed")]
    [InlineData(503, "replica_behind", false, "Failed")]
    [InlineData(503, "write_refused", false, "Failed")]
    [InlineData(503, null, false, "Unknown")]
    [InlineData(400, null, false, "Unknown")]
    [InlineData(500, null, false, "Unknown")]
    [InlineData(502, null, false, "Unknown")]
    [InlineData(504, null, false, "Unknown")]
    public void A_write_answer_is_classified_by_what_it_says_of_the_write(int status, string? errorCode, bool hasSeq, string expected) =>
        Assert.Equal(Enum.Parse<Outcome>(expected), ConfigClient.Classify((HttpStatusCode)status, errorCode, hasSeq));

    [Fact]
    public async Task A_correct_cluster_is_accepted_with_its_unknown_and_refused_writes_in_the_history()
    {
        var (verdict, counts) = await RunAsync(Mode.Correct);

        Assert.True(verdict.Linearizable, verdict.ToString());
        Assert.True(counts.Reads > 50 && counts.Writes > 10, counts.ToString());
        Assert.True(counts.UnknownWrites > 0 && counts.FailedWrites > 0 && counts.FailedReads > 0, counts.ToString());
    }

    [Fact]
    public async Task A_torn_read_across_two_types_is_rejected()
    {
        var (verdict, _) = await RunAsync(Mode.Torn);

        Assert.True(verdict.Violation, verdict.ToString());
    }

    [Fact]
    public async Task A_read_that_misses_an_acknowledged_write_is_rejected()
    {
        var (verdict, _) = await RunAsync(Mode.Stale);

        Assert.True(verdict.Violation, verdict.ToString());
    }

    [Fact]
    public async Task The_history_file_has_a_header_and_one_line_per_operation()
    {
        var path = HistoryPath();
        var registers = new ConfigRegisters();
        using (var recorder = new HistoryRecorder(path, registers.Names))
        {
            var client = new ConfigClient(new HttpClient(new Server(Mode.Correct)) { BaseAddress = new Uri("http://stand-in/") }, recorder, registers);
            await client.WriteAsync(1, 1, "r1", new Dictionary<string, int> { ["q0"] = 1, ["s2"] = 1 }, Limit);
            await client.ReadAsync(2, 2, null, Limit);
        }

        var lines = File.ReadAllLines(path);
        Assert.Equal(3, lines.Length);
        using var header = JsonDocument.Parse(lines[0]);
        Assert.Equal("reeve-history/1", header.RootElement.GetProperty("format").GetString());
        Assert.Equal(registers.Names, header.RootElement.GetProperty("types").EnumerateArray().Select(t => t.GetString()!).ToArray());
        using var write = JsonDocument.Parse(lines[1]);
        Assert.Equal("write", write.RootElement.GetProperty("kind").GetString());
        Assert.Equal("ok", write.RootElement.GetProperty("status").GetString());
        Assert.Equal(1, write.RootElement.GetProperty("writes").GetProperty("s2").GetInt32());
        using var read = JsonDocument.Parse(lines[2]);
        Assert.Equal(6, read.RootElement.GetProperty("reads").EnumerateObject().Count());
        Assert.True(read.RootElement.GetProperty("call").GetInt64() >= write.RootElement.GetProperty("ret").GetInt64());
    }

    private static string HistoryPath() => Path.Combine(LinearizabilityChecker.HistoryDirectory, $"recorder-{Guid.NewGuid():N}.jsonl");

    private static async Task<(CheckerVerdict Verdict, HistoryCounts Counts)> RunAsync(Mode mode)
    {
        var path = HistoryPath();
        var registers = new ConfigRegisters();
        HistoryCounts counts;
        using (var recorder = new HistoryRecorder(path, registers.Names))
        {
            var http = new HttpClient(new Server(mode)) { BaseAddress = new Uri("http://stand-in/") };
            var workload = new ConfigWorkload(new ConfigClient(http, recorder, registers), new Uri("http://stand-in/hub"), recorder, registers, ["r1", "r2"], new RunSeed(42), Limit);
            await workload.SeedAsync();
            using var stop = new CancellationTokenSource();
            var clients = workload.RunAsync(2, 2, 0, stop.Token);
            await Task.Delay(TimeSpan.FromSeconds(1));
            await WorkloadWait.UntilAsync(() => recorder.Counts is { Writes: >= 400, Reads: >= 400 }, TimeSpan.FromSeconds(120));
            await stop.CancelAsync();
            await clients;
            counts = recorder.Counts;
        }

        return (await LinearizabilityChecker.CheckAsync(path, Limit), counts);
    }

    private enum Mode
    {
        Correct,

        Torn,

        Stale,
    }

    private sealed class Server(Mode mode) : HttpMessageHandler
    {
        private readonly object gate = new();
        private readonly Dictionary<string, int> quotas = [];
        private readonly Dictionary<string, int> settings = [];
        private Dictionary<string, int> staleQuotas = [];
        private Dictionary<string, int> staleSettings = [];
        private int writes, reads;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            var response = request.Method == HttpMethod.Post ? await WriteAsync(request, cancellationToken).ConfigureAwait(false) : Read();
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            return response;
        }

        private async Task<HttpResponseMessage> WriteAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = await request.Content!.ReadFromJsonAsync<JsonElement>(cancellationToken).ConfigureAwait(false);
            var n = Interlocked.Increment(ref writes);
            if (n % 23 == 0)
            {
                return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = JsonContent.Create(new { errorCode = n % 2 == 0 ? "write_refused" : "replica_fenced" }) };
            }

            if (mode == Mode.Torn)
            {
                Apply(body, quotas: true, settings: false);
                await Task.Delay(2, cancellationToken).ConfigureAwait(false);
                Apply(body, quotas: false, settings: true);
            }
            else
            {
                Apply(body, quotas: true, settings: true);
            }

            if (mode == Mode.Stale)
            {
                _ = Task.Delay(20, CancellationToken.None).ContinueWith(
                    _ =>
                    {
                        lock (gate)
                        {
                            staleQuotas = new Dictionary<string, int>(quotas);
                            staleSettings = new Dictionary<string, int>(settings);
                        }
                    },
                    TaskScheduler.Default);
            }

            return n % 17 == 0 ? throw new TaskCanceledException() : new HttpResponseMessage(HttpStatusCode.OK);
        }

        private HttpResponseMessage Read()
        {
            if (Interlocked.Increment(ref reads) % 29 == 0)
            {
                return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = JsonContent.Create(new { errorCode = "replica_behind" }) };
            }

            lock (gate)
            {
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = mode == Mode.Stale
                        ? JsonContent.Create(new { quotas = new Dictionary<string, int>(staleQuotas), settings = new Dictionary<string, int>(staleSettings) })
                        : JsonContent.Create(new { quotas = new Dictionary<string, int>(quotas), settings = new Dictionary<string, int>(settings) }),
                };
            }
        }

        private void Apply(JsonElement body, bool quotas, bool settings)
        {
            lock (gate)
            {
                foreach (var (property, target, wanted) in new[] { ("quotas", this.quotas, quotas), ("settings", this.settings, settings) })
                {
                    foreach (var pair in wanted ? body.GetProperty(property).EnumerateObject() : [])
                    {
                        target[pair.Name] = pair.Value.GetInt32();
                    }
                }
            }
        }
    }
}
