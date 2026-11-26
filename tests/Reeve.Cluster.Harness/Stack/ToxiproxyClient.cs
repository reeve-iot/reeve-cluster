using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace Reeve.Cluster.Harness.Stack;

internal sealed class ToxiproxyClient(HttpClient http, Action<string>? log = null)
{
    public static readonly TimeSpan RepointBudget = TimeSpan.FromMinutes(4);

    public static readonly TimeSpan DeleteBudget = TimeSpan.FromMinutes(1);

    private static readonly TimeSpan RetryGap = TimeSpan.FromSeconds(1);

    public async Task CreateProxyAsync(string name, string listen, string upstream, CancellationToken cancellationToken = default)
    {
        using var response = await http
            .PostAsJsonAsync("/proxies", new ProxyBody(name, listen, upstream, true), cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, $"create proxy {name}").ConfigureAwait(false);
    }

    public async Task SetEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        using var response = await http
            .PostAsJsonAsync($"/proxies/{Uri.EscapeDataString(name)}", new EnabledBody(enabled), cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, $"set proxy {name} enabled={enabled}").ConfigureAwait(false);
    }

    public async Task SetUpstreamAsync(string name, string upstream, bool enabled, CancellationToken cancellationToken = default)
    {
        var what = $"point proxy {name} at {upstream}";
        Task<HttpResponseMessage> Send() => http.PostAsJsonAsync($"/proxies/{Uri.EscapeDataString(name)}", new UpstreamBody(upstream, enabled), cancellationToken);
        var (response, retried) = await SendWhileTimingOutAsync(what, RepointBudget, Send, cancellationToken).ConfigureAwait(false);
        using (response)
        {
            await EnsureAsync(response, what).ConfigureAwait(false);
        }

        if (retried)
        {
            var (last, _) = await SendWhileTimingOutAsync(what, RepointBudget, Send, cancellationToken).ConfigureAwait(false);
            using (last)
            {
                await EnsureAsync(last, what).ConfigureAwait(false);
            }
        }
    }

    public async Task AddToxicAsync(
        string proxy,
        string toxic,
        string type,
        string stream,
        IReadOnlyDictionary<string, object> attributes,
        CancellationToken cancellationToken = default)
    {
        using var response = await http
            .PostAsJsonAsync($"/proxies/{Uri.EscapeDataString(proxy)}/toxics", new ToxicBody(toxic, type, stream, 1.0f, attributes), cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, $"add toxic {toxic} ({type}, {stream}) to {proxy}").ConfigureAwait(false);
    }

    public async Task RemoveToxicAsync(string proxy, string toxic, CancellationToken cancellationToken = default)
    {
        using var response = await http
            .DeleteAsync($"/proxies/{Uri.EscapeDataString(proxy)}/toxics/{Uri.EscapeDataString(toxic)}", cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, $"remove toxic {toxic} from {proxy}").ConfigureAwait(false);
    }

    public async Task DeleteProxyAsync(string name, CancellationToken cancellationToken = default)
    {
        var what = $"delete proxy {name}";
        var (response, retried) = await SendWhileTimingOutAsync(
            what,
            DeleteBudget,
            () => http.DeleteAsync($"/proxies/{Uri.EscapeDataString(name)}", cancellationToken),
            cancellationToken).ConfigureAwait(false);
        using (response)
        {
            if (retried && response.StatusCode == HttpStatusCode.NotFound)
            {
                return;
            }

            await EnsureAsync(response, what).ConfigureAwait(false);
        }
    }

    private async Task<(HttpResponseMessage Response, bool Retried)> SendWhileTimingOutAsync(string what, TimeSpan budget, Func<Task<HttpResponseMessage>> send, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        for (var attempt = 1; ; attempt++)
        {
            var response = await send().ConfigureAwait(false);
            if (response.StatusCode != HttpStatusCode.ServiceUnavailable || clock.Elapsed >= budget)
            {
                return (response, attempt > 1);
            }

            response.Dispose();
            log?.Invoke($"toxiproxy: {what} answered 503 after {clock.Elapsed.TotalSeconds:F0} s (attempt {attempt}); again");
            await Task.Delay(RetryGap, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task EnsureAsync(HttpResponseMessage response, string what)
    {
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"Toxiproxy refused to {what}: {(int)response.StatusCode} {body}");
        }
    }

    private sealed record ProxyBody(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("listen")] string Listen,
        [property: JsonPropertyName("upstream")] string Upstream,
        [property: JsonPropertyName("enabled")] bool Enabled);

    private sealed record UpstreamBody(
        [property: JsonPropertyName("upstream")] string Upstream,
        [property: JsonPropertyName("enabled")] bool Enabled);

    private sealed record EnabledBody([property: JsonPropertyName("enabled")] bool Enabled);

    private sealed record ToxicBody(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("stream")] string Stream,
        [property: JsonPropertyName("toxicity")] float Toxicity,
        [property: JsonPropertyName("attributes")] IReadOnlyDictionary<string, object> Attributes);
}
