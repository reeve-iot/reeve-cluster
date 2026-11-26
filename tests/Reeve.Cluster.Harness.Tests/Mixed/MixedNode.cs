using System.Text;
using System.Text.Json;
using Reeve.Cluster.Harness.Processes;

namespace Harness.Mixed;

internal sealed class MixedNode(SampleProcess process, string build)
{
    public SampleProcess Process { get; } = process;

    public string Id => Process.Id;

    public string Build { get; } = build;

    public bool Stopping { get; private set; }

    public void MarkStopping() => Stopping = true;

    public async Task<string> VersionAsync() => (await GetAsync("mixed/whoami")).GetProperty("version").GetString()!;

    public async Task<string> LibraryVersionAsync() => (await GetAsync("mixed/whoami")).GetProperty("library").GetString()!;

    public async Task<IReadOnlyDictionary<string, long>> TenuresAsync() =>
        (await GetAsync("mixed/units")).EnumerateArray().ToDictionary(u => $"{u.GetProperty("kind").GetString()}/{u.GetProperty("key").GetString()}", u => u.GetProperty("token").GetInt64());

    public async Task<IReadOnlyDictionary<string, long>> TenuresOrNoneAsync()
    {
        if (!Stopping)
        {
            return await TenuresAsync().ConfigureAwait(false);
        }

        if (Process.HasExited)
        {
            return new Dictionary<string, long>();
        }

        try
        {
            return await TenuresAsync().ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or InvalidOperationException)
        {
            return new Dictionary<string, long>();
        }
    }

    public async Task<string> CreateKeyAsync(string key) => (await PostAsync($"mixed/keyed/{key}")).GetProperty("outcome").GetString()!;

    public Task<JsonElement> AskMemberAsync(string target, string payload) => PostAsync($"mixed/ask/member/{target}", payload);

    public Task<JsonElement> AskKeyAsync(string key, string payload) => PostAsync($"mixed/ask/key/{key}", payload);

    public Task<JsonElement> ClaimAsync(string store, string key) => PostAsync($"mixed/claim/{store}/{key}");

    public async Task<bool> ReleaseClaimAsync(string store, string key, Guid token) =>
        (await SendAsync(HttpMethod.Delete, $"mixed/claim/{store}/{key}/{token}")).GetProperty("released").GetBoolean();

    public Task<JsonElement> AcquireSlotAsync(string store, string key, string id, int limit) => PostAsync($"mixed/slot/{store}/{key}/{id}/{limit}");

    public async Task<bool> ReleaseSlotAsync(string store, string key, string id, Guid token) =>
        (await SendAsync(HttpMethod.Delete, $"mixed/slot/{store}/{key}/{id}/{token}")).GetProperty("released").GetBoolean();

    public async Task<bool> RequireVersionAsync(string version) => (await PostAsync($"mixed/gate/require/{version}")).GetProperty("raised").GetBoolean();

    public async Task<bool> AllMembersAtLeastAsync(string version) => (await GetAsync($"mixed/gate/at-least/{version}")).GetProperty("atLeast").GetBoolean();

    public async Task<System.Net.HttpStatusCode> PutFlagAsync(string name, bool enabled)
    {
        using var response = await Process.Client.PutAsync($"flags/{name}", Json($$"""{"enabled":{{(enabled ? "true" : "false")}}}""")).ConfigureAwait(false);
        return response.StatusCode;
    }

    public async Task<bool?> FlagAsync(string name)
    {
        using var response = await Process.Client.GetAsync($"flags/{name}").ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).RootElement.GetProperty("enabled").GetBoolean();
    }

    public async Task<bool?> AppliedFlagAsync(string name)
    {
        using var response = await Process.Client.GetAsync($"mixed/flag/{name}").ConfigureAwait(false);
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)).RootElement.GetProperty("enabled").GetBoolean();
    }

    public async Task<(int? Quota, int? Setting)> AppliedConfigAsync(string tenant, string setting)
    {
        var view = await GetAsync("mixed/config").ConfigureAwait(false);
        return (
            view.GetProperty("quotas").TryGetProperty(tenant, out var quota) ? quota.GetInt32() : null,
            view.GetProperty("settings").TryGetProperty(setting, out var value) ? value.GetInt32() : null);
    }

    public static StringContent Json(string json) => new(json, Encoding.UTF8, "application/json");

    private Task<JsonElement> GetAsync(string path) => SendAsync(HttpMethod.Get, path);

    private Task<JsonElement> PostAsync(string path, string? body = null) => SendAsync(HttpMethod.Post, path, body);

    private async Task<JsonElement> SendAsync(HttpMethod method, string path, string? body = null)
    {
        using var request = new HttpRequestMessage(method, path) { Content = body is null ? null : new StringContent(body, Encoding.UTF8, "text/plain") };
        using var response = await Process.Client.SendAsync(request).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        return response.IsSuccessStatusCode
            ? JsonDocument.Parse(text).RootElement.Clone()
            : throw new InvalidOperationException($"{method} {path} on {Id} ({Build}) answered {(int)response.StatusCode}: {text}");
    }
}
