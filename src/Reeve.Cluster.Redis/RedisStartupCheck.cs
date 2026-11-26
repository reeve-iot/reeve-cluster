using System.Net;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Redis.Primitives;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis;

internal sealed class RedisStartupCheck(
    IConnectionMultiplexer redis,
    IEnumerable<UnitKind> kinds,
    IEnumerable<RedisRequirement> requirementsRegistered,
    IOptions<ClusterRedisOptions> options,
    ILogger<RedisStartupCheck> logger) : IHostedLifecycleService
{
    internal static readonly Version Floor = new(6, 2, 5);

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        var hasRedisFence = kinds.Any(k => k.Options.RedisFence);
        var registered = requirementsRegistered.ToList();
        var requirements = registered.Where(r => r.NeedsVersionFloor).Select(r => r.Primitive).Distinct().ToList();

        var checksPolicy = hasRedisFence || registered.Count > 0;
        var refusesUnreachable = hasRedisFence || registered.Any(r => r.RefusesUnreachable);
        if (checksPolicy && !options.Value.CheckEvictionPolicy)
        {
            logger.LogWarning("ClusterRedis:CheckEvictionPolicy is off: the library does not check that Redis will not evict the keys of FenceRedis, claims, single-flight, budgets, counters and held slots. The deployment is responsible for keeping maxmemory-policy at noeviction.");
        }

        var primaries = redis.GetServers().Where(s => s.IsConnected && !s.IsReplica).ToList();
        if (primaries.Count == 0)
        {
            if (refusesUnreachable && options.Value.CheckEvictionPolicy)
            {
                throw Refusal("No connected Redis primary is available to check its eviction policy.");
            }

            logger.LogWarning("Redis is not connected at start; its version and eviction policy are not checked (RedisFence, a Redis claim, a Redis single-flight and a Redis slot need Redis 6.2.5 or later; every Redis primitive needs maxmemory-policy noeviction).");
            return;
        }

        if (hasRedisFence || requirements.Count > 0)
        {
            var needs = string.Join(", ", (hasRedisFence ? new[] { "RedisFence" } : []).Concat(requirements.Select(p => $"the Redis {p}")));
            foreach (var server in primaries)
            {
                Throw(EvaluateVersion(server.EndPoint, server.Version, needs));
            }
        }

        if (options.Value.CheckEvictionPolicy && checksPolicy)
        {
            await CheckPoliciesAsync(refusesUnreachable, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    internal static ClusterConfigurationException? EvaluateVersion(EndPoint endPoint, Version version, string needs = "RedisFence") =>
        version < Floor
            ? new ClusterConfigurationException(
                ClusterConfigurationReason.RedisTooOld,
                $"Redis {endPoint} runs {version}; {needs} needs 6.2.5 or later.")
            : null;

    internal static ClusterConfigurationException? EvaluatePolicy(EndPoint endPoint, string? policy) =>
        policy == "noeviction"
            ? null
            : Refusal($"Redis {endPoint} has maxmemory-policy {policy ?? "(not reported)"}, which may evict a claim, single-flight, budget, counter, slot or FenceRedis key; it must be noeviction.");

    private async Task CheckPoliciesAsync(bool refusesUnreachable, CancellationToken cancellationToken)
    {
        try
        {
            var configuration = ConfigurationOptions.Parse(redis.Configuration);
            configuration.AllowAdmin = true;
            configuration.AbortOnConnectFail = true;
            var admin = await ConnectionMultiplexer.ConnectAsync(configuration).WaitAsync(cancellationToken).ConfigureAwait(false);
            await using (admin.ConfigureAwait(false))
            {
                var servers = admin.GetServers();
                foreach (var unreached in servers.Where(s => !s.IsConnected))
                {
                    if (refusesUnreachable)
                    {
                        throw Refusal($"The eviction policy of Redis {unreached.EndPoint} could not be read: the server is not connected (it may be a replica).");
                    }

                    logger.LogWarning("The eviction policy of Redis {EndPoint} was not read: the server is not connected (it may be a replica).", unreached.EndPoint);
                }

                if (!servers.Any(s => !s.IsReplica))
                {
                    throw Refusal("The check's connection lists no primary, so no eviction policy could be read.");
                }

                foreach (var server in servers.Where(s => s.IsConnected && !s.IsReplica))
                {
                    var memory = await server.InfoAsync("memory").WaitAsync(cancellationToken).ConfigureAwait(false);
                    var policy = memory.SelectMany(section => section).FirstOrDefault(field => field.Key == "maxmemory_policy").Value;
                    Throw(EvaluatePolicy(server.EndPoint, policy));
                }
            }
        }
        catch (Exception e) when (e is RedisException or RedisTimeoutException)
        {
            throw Refusal($"The eviction policy of Redis could not be read with INFO memory ({e.GetType().Name}: {e.Message}).");
        }
        catch (Exception e) when (e is not ClusterConfigurationException && (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested))
        {
            throw Refusal($"The eviction policy check could not connect or read INFO memory ({e.GetType().Name}).");
        }
    }

    private static ClusterConfigurationException Refusal(string message) =>
        new(
            ClusterConfigurationReason.RedisEvictionPolicy,
            message + " ClusterRedisOptions.CheckEvictionPolicy = false turns the check off; the deployment then answers for the policy.");

    private static void Throw(ClusterConfigurationException? refusal)
    {
        if (refusal is not null)
        {
            throw refusal;
        }
    }
}
