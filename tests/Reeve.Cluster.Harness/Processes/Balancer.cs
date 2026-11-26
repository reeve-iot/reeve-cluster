using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Reeve.Cluster.Harness.Processes;

internal sealed record BalancerRequest(string Id, string Method, int ClientStatus, IReadOnlyList<(string Replica, int? Status)> Attempts, TimeSpan Elapsed);

internal sealed class Balancer : IAsyncDisposable
{
    public const string Image = "nginx:1.28-alpine";

    public const string FirstHeader = "X-Harness-First";

    public const string IdHeader = "X-Harness-Id";

    private const string Loopback = "127.0.0.1";
    private const long MemoryBytes = 64L * 1024 * 1024;

    private readonly IContainer container;
    private readonly IReadOnlyDictionary<int, string> replicaOfPort;

    private Balancer(IContainer container, int port, IReadOnlyDictionary<int, string> replicaOfPort)
    {
        this.container = container;
        this.replicaOfPort = replicaOfPort;
        Url = new Uri($"http://{Loopback}:{port}/");
    }

    public Uri Url { get; }

    public static string NetworkMode
    {
        get
        {
            if (!File.Exists("/.dockerenv"))
            {
                return "host";
            }

            var id = Regex.Match(File.ReadAllText("/proc/self/mountinfo"), "containers/([0-9a-f]{64})/").Groups[1].Value;
            return $"container:{(id.Length == 0 ? Environment.MachineName : id)}";
        }
    }

    public static string Configuration(IReadOnlyList<(string Id, int Port)> replicas, int port, TimeSpan proxyReadTimeout)
    {
        string Server(int port, string suffix = "") => $"server {Loopback}:{port} max_fails=0{suffix};";
        var all = string.Join(" ", replicas.Select(r => Server(r.Port)));
        var first = string.Join(Environment.NewLine, replicas.Select(r =>
            $"    upstream first_{r.Id} {{ {Server(r.Port)} {string.Join(" ", replicas.Where(o => o.Id != r.Id).Select(o => Server(o.Port, " backup")))} }}"));
        var pools = string.Join(" ", replicas.Select(r => $"{r.Id} first_{r.Id};"));
        var read = proxyReadTimeout.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture);
        return $$"""
            worker_processes 1;
            events { worker_connections 1024; }
            http {
                log_format harness '$http_x_harness_id|$request_method|$status|$upstream_addr|$upstream_status|$request_time';
                access_log /dev/stdout harness;
                error_log /dev/stderr info;

                map $http_upgrade $connection_upgrade { default upgrade; '' close; }
                map $http_x_harness_first $pool { default cluster; {{pools}} }

                # max_fails=0: a replica that failed stays in rotation, so each request meets the failure under test.
                upstream cluster { {{all}} }
            {{first}}

                server {
                    listen {{Loopback}}:{{port}};
                    proxy_http_version 1.1;
                    proxy_set_header Host $host;
                    proxy_set_header Upgrade $http_upgrade;
                    proxy_set_header Connection $connection_upgrade;
                    proxy_connect_timeout 30s;
                    proxy_send_timeout {{read}}s;
                    proxy_read_timeout {{read}}s;
                    # The spec's balancer: connection failures and the three 503s, idempotent methods only, never a read timeout.
                    proxy_next_upstream error http_503;
                    location / { proxy_pass http://$pool; }
                }
            }
            """;
    }

    public static async Task<Balancer> StartAsync(IReadOnlyList<(string Id, int Port)> replicas, int port, TimeSpan proxyReadTimeout, CancellationToken cancellationToken = default)
    {
        var container = new ContainerBuilder(Image)
            .WithResourceMapping(Encoding.UTF8.GetBytes(Configuration(replicas, port, proxyReadTimeout)), FilePath.Of("/etc/nginx/nginx.conf"))
            .WithCreateParameterModifier(p =>
            {
                var host = p.HostConfig ?? throw new InvalidOperationException("The container builder passed create parameters without a host configuration.");
                host.NetworkMode = NetworkMode;
                host.Memory = MemoryBytes;
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(port))
            .Build();
        await container.StartAsync(cancellationToken).ConfigureAwait(false);
        return new Balancer(container, port, replicas.ToDictionary(r => r.Port, r => r.Id));
    }

    public async Task<BalancerRequest> RequestAsync(string id, TimeSpan? limit = null)
    {
        var deadline = DateTime.UtcNow + (limit ?? TimeSpan.FromSeconds(30));
        while (true)
        {
            var (stdout, _) = await container.GetLogsAsync().ConfigureAwait(false);
            foreach (var line in stdout.Split('\n'))
            {
                var space = line.IndexOf(' ');
                var fields = (space > 0 && line[space - 1] == 'Z' ? line[(space + 1)..] : line).Split('|');
                if (fields.Length == 6 && fields[0] == id)
                {
                    return Parse(fields);
                }
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"The balancer logged no request '{id}'.");
            }

            await Task.Delay(50).ConfigureAwait(false);
        }
    }

    public async Task<string> LogAsync(int tail = 400)
    {
        var (stdout, stderr) = await container.GetLogsAsync().ConfigureAwait(false);
        return $"--- balancer access log (last {tail} lines){Environment.NewLine}{Tail(stdout, tail)}{Environment.NewLine}--- balancer error log (last {tail} lines){Environment.NewLine}{Tail(stderr, tail)}";
    }

    private static string Tail(string text, int lines) =>
        string.Join(Environment.NewLine, text.Split('\n').TakeLast(lines));

    private BalancerRequest Parse(string[] fields)
    {
        var addresses = fields[3].Split(", ", StringSplitOptions.RemoveEmptyEntries);
        var statuses = fields[4].Split(", ", StringSplitOptions.RemoveEmptyEntries);
        var attempts = new List<(string Replica, int? Status)>();
        for (var i = 0; i < addresses.Length; i++)
        {
            var port = int.Parse(addresses[i][(addresses[i].LastIndexOf(':') + 1)..], CultureInfo.InvariantCulture);
            attempts.Add((replicaOfPort[port], int.TryParse(i < statuses.Length ? statuses[i] : null, CultureInfo.InvariantCulture, out var status) ? status : null));
        }

        return new BalancerRequest(fields[0], fields[1], int.Parse(fields[2], CultureInfo.InvariantCulture), attempts, TimeSpan.FromSeconds(double.Parse(fields[5], CultureInfo.InvariantCulture)));
    }

    public ValueTask DisposeAsync() => container.DisposeAsync();
}
