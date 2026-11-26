using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Runtime.InteropServices;
using Reeve.Cluster.Harness.Stack;

namespace Reeve.Cluster.Harness.Processes;

internal enum ProcessSignal
{
    Term = 15,

    Kill = 9,

    Stop = 19,

    Cont = 18,
}

internal sealed class SampleProcess : IAsyncDisposable
{
    private const int OutputLines = 400;

    private const string MarkerPrefix = "harness: ";

    private readonly Process process;
    private readonly object gate = new();
    private readonly Queue<string> output = new();
    private readonly Dictionary<string, int> markers = new(StringComparer.Ordinal);
    private readonly Action<string> log;

    private SampleProcess(string id, int port, ReplicaLink link, ClusterOptions options, Process process, Action<string> log)
    {
        Id = id;
        Port = port;
        Link = link;
        Options = options;
        this.process = process;
        this.log = log;
        Url = new Uri($"http://127.0.0.1:{port}/");
        Client = new HttpClient { BaseAddress = Url, Timeout = TimeSpan.FromSeconds(90) };
    }

    public string Id { get; }

    public int Port { get; }

    public Uri Url { get; }

    public ReplicaLink Link { get; }

    public ClusterOptions Options { get; }

    public HttpClient Client { get; }

    public bool HasExited => process.HasExited;

    public int ExitCode => process.ExitCode;

    public int MarkerCount(string marker)
    {
        lock (gate)
        {
            return markers.GetValueOrDefault(marker);
        }
    }

    public IReadOnlyList<string> Output
    {
        get
        {
            lock (gate)
            {
                return [.. output];
            }
        }
    }

    public static string Framework => new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)).Name;

    public static string SampleAssemblyPath
    {
        get
        {
            var configuration = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)).Parent!.Name;
            var path = Path.Combine(RepositoryRoot, "samples", "Reeve.Cluster.Sample", "bin", configuration, Framework, "Reeve.Cluster.Sample.dll");
            return File.Exists(path) ? path : throw new FileNotFoundException($"The sample is not built: {path}. Build the solution first.");
        }
    }

    public static string RepositoryRoot
    {
        get
        {
            var root = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
            while (root is not null && !File.Exists(Path.Combine(root.FullName, "Reeve.Cluster.slnx")))
            {
                root = root.Parent;
            }

            return root?.FullName ?? throw new InvalidOperationException("The repository root was not found above the test assembly.");
        }
    }

    public static SampleProcess Start(string id, int port, ReplicaLink link, ClusterOptions options, Action<string> log, string? assembly = null, IEnumerable<KeyValuePair<string, string>>? extraEnvironment = null, IEnumerable<string>? arguments = null)
    {
        if (!OperatingSystem.IsLinux())
        {
            throw new PlatformNotSupportedException("Process mode sends Linux signals.");
        }

        options.ConnectionString = link.PostgresConnectionString;
        assembly ??= SampleAssemblyPath;
        var info = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = Path.GetDirectoryName(assembly)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add(assembly);
        foreach (var argument in arguments ?? [])
        {
            info.ArgumentList.Add(argument);
        }

        foreach (var (key, value) in Environment(id, port, options))
        {
            info.Environment[key] = value;
        }

        foreach (var (key, value) in extraEnvironment ?? [])
        {
            info.Environment[key] = value;
        }

        var process = new Process { StartInfo = info, EnableRaisingEvents = true };
        var started = new SampleProcess(id, port, link, options, process, log);
        process.OutputDataReceived += (_, e) => started.Record(e.Data);
        process.ErrorDataReceived += (_, e) => started.Record(e.Data);
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        return started;
    }

    internal static IEnumerable<KeyValuePair<string, string>> Environment(string id, int port, ClusterOptions options)
    {
        yield return new("ASPNETCORE_URLS", $"http://127.0.0.1:{port}");
        yield return new("Cluster__ReplicaId", id);
        yield return new("Cluster__ConnectionString", options.ConnectionString!);
        yield return new("Cluster__CheckSynchronousReplication", options.CheckSynchronousReplication ? "true" : "false");
        foreach (var (name, value) in new (string, TimeSpan)[]
        {
            ("LeaseTtl", options.LeaseTtl),
            ("SafetyMargin", options.SafetyMargin),
            ("StopBudget", options.StopBudget),
            ("RenewTimeout", options.RenewTimeout),
            ("RenewInterval", options.RenewInterval),
            ("ReconcileTimeout", options.ReconcileTimeout!.Value),
            ("ApplyStallLimit", options.ApplyStallLimit),
            ("AckTimeout", options.AckTimeout),
            ("AckPollInterval", options.AckPollInterval),
            ("ReadIndexTimeout", options.ReadIndexTimeout),
            ("ActionTimeBudget", options.ActionTimeBudget!.Value),
            ("ProxyReadTimeout", options.ProxyReadTimeout!.Value),
            ("RejoinBackoff", options.RejoinBackoff),
        })
        {
            yield return new($"Cluster__{name}", value.ToString("c", CultureInfo.InvariantCulture));
        }
    }

    public void Signal(ProcessSignal signal)
    {
        if (process.HasExited)
        {
            return;
        }

        log($"{Id}: signal {signal} to pid {process.Id}");
        if (NativeMethods.Kill(process.Id, (int)signal) != 0)
        {
            throw new InvalidOperationException($"kill({process.Id}, {signal}) failed: errno {Marshal.GetLastWin32Error()}.");
        }
    }

    public async Task<int> WaitForExitAsync(TimeSpan limit)
    {
        using var timeout = new CancellationTokenSource(limit);
        try
        {
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"{Id} did not exit within {limit}.");
        }

        return process.ExitCode;
    }

    public async Task WaitReadyAsync(TimeSpan limit)
    {
        var deadline = DateTime.UtcNow + limit;
        while (true)
        {
            if (process.HasExited)
            {
                throw new InvalidOperationException($"{Id} exited with {process.ExitCode} before it served.{System.Environment.NewLine}{string.Join(System.Environment.NewLine, Output)}");
            }

            try
            {
                using var response = await Client.GetAsync("health/ready").ConfigureAwait(false);
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return;
                }
            }
            catch (HttpRequestException)
            {
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"{Id} did not serve within {limit}.{System.Environment.NewLine}{string.Join(System.Environment.NewLine, Output)}");
            }

            await Task.Delay(100).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        if (!process.HasExited)
        {
            process.Kill(entireProcessTree: true);
        }

        await process.WaitForExitAsync().ConfigureAwait(false);
        process.Dispose();
    }

    private void Record(string? line)
    {
        if (line is null)
        {
            return;
        }

        lock (gate)
        {
            if (line.StartsWith(MarkerPrefix, StringComparison.Ordinal))
            {
                var marker = line[MarkerPrefix.Length..];
                markers[marker] = markers.GetValueOrDefault(marker) + 1;
            }

            output.Enqueue(line);
            while (output.Count > OutputLines)
            {
                output.Dequeue();
            }
        }
    }

    private static class NativeMethods
    {
        [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
        internal static extern int Kill(int pid, int signal);
    }
}
