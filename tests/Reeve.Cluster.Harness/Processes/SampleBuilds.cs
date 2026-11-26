using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;

namespace Reeve.Cluster.Harness.Processes;

internal sealed record SampleBuild(string Name, string AssemblyPath)
{
    public IEnumerable<KeyValuePair<string, string>> Environment(HarnessRun run, ReplicaLink link) =>
    [
        new("Sample__MixedFleet", "true"),
        new("Sample__RedisConnectionString", link.RedisConnectionString),
        new("Sample__RedisKeyPrefix", run.RedisKeyPrefix),
        new("Cluster__PlacementInterval", "00:00:01"),
        new("Cluster__StabilisationWindow", "00:00:03"),
        new("DOTNET_ROLL_FORWARD", "Major"),
    ];
}

internal static class SampleBuilds
{
    private static readonly string[] Exported = ["src", "samples", "Directory.Build.props", "Directory.Build.targets", "Directory.Packages.props", "global.json", "nuget.config", ".editorconfig"];
    private static readonly string[] Required = ["src", "samples"];
    private static readonly string[] PublishArguments = ["-c", "Release", "-nologo", "-v", "minimal", "--disable-build-servers"];
    private static readonly ConcurrentDictionary<string, Lazy<Task<SampleBuild>>> Cache = new(StringComparer.Ordinal);

    public static SampleBuild Current() => new("current", SampleProcess.SampleAssemblyPath);

    public static Task<SampleBuild> FromRefAsync(string gitRef, Action<string> log) =>
        Cache.GetOrAdd(gitRef, _ => new Lazy<Task<SampleBuild>>(() => Task.Run(() => BuildAsync(gitRef, log)))).Value;

    private static async Task<SampleBuild> BuildAsync(string gitRef, Action<string> log)
    {
        var root = SampleProcess.RepositoryRoot;
        var commit = (await RunAsync("git", ["-C", root, "rev-parse", "--verify", "--quiet", $"{gitRef}^{{commit}}"], root, TimeSpan.FromMinutes(1), log).ConfigureAwait(false)).Trim();
        var overlay = Path.Combine(root, "samples", "Reeve.Cluster.Sample");
        var key = $"{commit[..12]}-{FingerprintOf(overlay)}";
        var directory = Path.Combine(System.Environment.GetEnvironmentVariable("REEVE_MIXED_BUILD_DIR") is { Length: > 0 } configured ? configured : Path.Combine(Path.GetTempPath(), "reeve-cluster-mixed-version"), key);
        var published = Path.Combine(directory, "out");
        var assembly = Path.Combine(published, "Reeve.Cluster.Sample.dll");
        if (File.Exists(assembly))
        {
            return new SampleBuild(gitRef, assembly);
        }

        log($"mixed-version: building the sample over {gitRef} ({commit[..12]}) in {directory}");
        var source = Path.Combine(directory, "src");
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }

        Directory.CreateDirectory(source);
        var present = (await RunAsync("git", ["-C", root, "ls-tree", "--name-only", commit], root, TimeSpan.FromMinutes(1), log).ConfigureAwait(false)).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (Required.FirstOrDefault(r => !present.Contains(r)) is { } missing)
        {
            throw new InvalidOperationException($"{gitRef} has no {missing} at its root.");
        }

        var paths = string.Join(' ', Exported.Where(present.Contains).Select(p => $"'{p}'"));
        await RunAsync("bash", ["-c", $"set -o pipefail; git -C '{root}' archive '{commit}' {paths} | tar -x -C '{source}'"], root, TimeSpan.FromMinutes(2), log).ConfigureAwait(false);

        var target = Path.Combine(source, "samples", "Reeve.Cluster.Sample");
        Directory.Delete(target, recursive: true);
        CopyDirectory(overlay, target);

        var project = Path.Combine(target, "Reeve.Cluster.Sample.csproj");
        var frameworks = (await RunAsync("dotnet", ["msbuild", project, "-nologo", "-getProperty:TargetFrameworks"], source, TimeSpan.FromMinutes(2), log).ConfigureAwait(false)).Trim().Split(';', StringSplitOptions.RemoveEmptyEntries);
        string[] framework = frameworks.Contains(SampleProcess.Framework, StringComparer.Ordinal) ? ["-f", SampleProcess.Framework] : [];
        await RunAsync("dotnet", ["publish", project, .. PublishArguments, .. framework, "-o", published], source, TimeSpan.FromMinutes(10), log).ConfigureAwait(false);
        return File.Exists(assembly) ? new SampleBuild(gitRef, assembly) : throw new FileNotFoundException($"The publish of the sample over {gitRef} produced no {assembly}.");
    }

    private static string FingerprintOf(string overlay)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.UTF8.GetBytes(string.Join('\n', [.. Exported, .. PublishArguments, SampleProcess.Framework])));
        foreach (var file in OverlayFiles(overlay))
        {
            hash.AppendData(Encoding.UTF8.GetBytes(Path.GetRelativePath(overlay, file)));
            hash.AppendData(File.ReadAllBytes(file));
        }

        return Convert.ToHexString(hash.GetHashAndReset())[..8].ToLowerInvariant();
    }

    private static IEnumerable<string> OverlayFiles(string overlay) =>
        Directory.EnumerateFiles(overlay, "*", SearchOption.AllDirectories)
            .Where(f => !Path.GetRelativePath(overlay, f).Split(Path.DirectorySeparatorChar)[0].Equals("bin", StringComparison.Ordinal)
                && !Path.GetRelativePath(overlay, f).Split(Path.DirectorySeparatorChar)[0].Equals("obj", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal);

    private static void CopyDirectory(string from, string to)
    {
        foreach (var file in OverlayFiles(from))
        {
            var destination = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(file, destination);
        }
    }

    private static async Task<string> RunAsync(string file, string[] arguments, string workingDirectory, TimeSpan limit, Action<string> log)
    {
        var info = new ProcessStartInfo(file)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            info.ArgumentList.Add(argument);
        }

        using var process = Process.Start(info)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(limit);
        try
        {
            await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"{file} {string.Join(' ', arguments)} did not end within {limit}.");
        }

        var text = await ReadAsync(output).ConfigureAwait(false);
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"{file} {string.Join(' ', arguments)} exited {process.ExitCode}.{System.Environment.NewLine}{string.Join(System.Environment.NewLine, (text + await ReadAsync(error).ConfigureAwait(false)).Split('\n').TakeLast(40))}");
        }

        log($"mixed-version: {file} {string.Join(' ', arguments.Take(2))} ok");
        return text;
    }

    private static async Task<string> ReadAsync(Task<string> read)
    {
        try
        {
            return await read.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return string.Empty;
        }
    }
}
