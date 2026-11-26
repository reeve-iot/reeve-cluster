using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Reeve.Cluster.Harness.Processes;

namespace Reeve.Cluster.Harness.Linearizability;

internal sealed class ConfigClient(HttpClient http, HistoryRecorder recorder, ConfigRegisters registers)
{
    public static Outcome Classify(HttpStatusCode status, string? errorCode, bool hasSeq) => status switch
    {
        _ when hasSeq => Outcome.Unknown,
        HttpStatusCode.OK => Outcome.Ok,
        HttpStatusCode.ServiceUnavailable when errorCode is "replica_not_serving" or "replica_fenced" or "replica_behind" or "write_refused" => Outcome.Failed,
        _ => Outcome.Unknown,
    };

    public async Task<Outcome> WriteAsync(int client, int replica, string? first, IReadOnlyDictionary<string, int> values, TimeSpan timeout)
    {
        var pending = recorder.BeginWrite(client, replica);
        var outcome = Outcome.Unknown;
        try
        {
            using var cancellation = new CancellationTokenSource(timeout);
            using var request = Request(HttpMethod.Post, first, pending.Id);
            request.Content = JsonContent.Create(registers.Body(values));
            using var response = await http.SendAsync(request, cancellation.Token).ConfigureAwait(false);
            outcome = Classify(response.StatusCode, await ErrorCodeAsync(response, cancellation.Token).ConfigureAwait(false), response.Headers.Contains("Reeve-Seq"));
        }
        catch (Exception exception) when (exception is HttpRequestException or OperationCanceledException or IOException)
        {
            outcome = Outcome.Unknown;
        }

        recorder.EndWrite(pending, outcome, values);
        return outcome;
    }

    public async Task<bool> ReadAsync(int client, int replica, string? first, TimeSpan timeout)
    {
        var pending = recorder.BeginRead(client, replica);
        JsonElement? answer = null;
        try
        {
            using var cancellation = new CancellationTokenSource(timeout);
            using var request = Request(HttpMethod.Get, first, pending.Id);
            using var response = await http.SendAsync(request, cancellation.Token).ConfigureAwait(false);
            if (response.StatusCode == HttpStatusCode.OK)
            {
                answer = await response.Content.ReadFromJsonAsync<JsonElement>(cancellation.Token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (exception is HttpRequestException or OperationCanceledException or IOException)
        {
            answer = null;
        }

        var values = answer is { } body ? registers.Parse(body) : null;
        recorder.EndRead(pending, values);
        return values is not null;
    }

    private static HttpRequestMessage Request(HttpMethod method, string? first, long id)
    {
        var request = new HttpRequestMessage(method, "config");
        request.Headers.Add(Balancer.IdHeader, $"lin-{id}");
        if (first is not null)
        {
            request.Headers.Add(Balancer.FirstHeader, first);
        }

        return request;
    }

    private static async Task<string?> ErrorCodeAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        if (body.Length == 0 || response.StatusCode == HttpStatusCode.OK)
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(body);
            return document.RootElement.TryGetProperty("errorCode", out var code) ? code.GetString() : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
