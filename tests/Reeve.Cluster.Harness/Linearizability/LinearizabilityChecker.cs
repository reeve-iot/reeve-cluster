using System.Diagnostics;
using System.Text;

namespace Reeve.Cluster.Harness.Linearizability;

internal sealed record CheckerVerdict(int ExitCode, string Summary, string Error, string? Visualization)
{
    public bool Linearizable => ExitCode == 0;

    public bool Violation => ExitCode == 1;

    public bool TimedOut => ExitCode == 2;

    public override string ToString() => ExitCode switch
    {
        0 => $"linearizable: {Summary}",
        1 => $"NOT LINEARIZABLE: {Summary}{(Visualization is null ? "" : $" (picture: {Visualization})")}",
        2 => $"NO VERDICT, the checker timed out: {Summary}",
        _ => $"the checker refused the history (exit {ExitCode}): {Error}",
    };
}

internal static class LinearizabilityChecker
{
    public const string BinaryVariable = "REEVE_LINEARIZABILITY_CHECKER";

    private static readonly Lazy<Task<string>> Binary = new(BuildAsync);

    public static string HistoryDirectory
    {
        get
        {
            var directory = Path.Combine(RepositoryRoot(), "artifacts", "linearizability");
            Directory.CreateDirectory(directory);
            return directory;
        }
    }

    public static async Task<CheckerVerdict> CheckAsync(string historyPath, TimeSpan timeout)
    {
        var visualization = Path.ChangeExtension(historyPath, ".html");
        var info = new ProcessStartInfo(await Binary.Value.ConfigureAwait(false))
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        info.ArgumentList.Add("-history");
        info.ArgumentList.Add(historyPath);
        info.ArgumentList.Add("-timeout");
        info.ArgumentList.Add($"{(long)timeout.TotalMilliseconds}ms");
        info.ArgumentList.Add("-viz");
        info.ArgumentList.Add(visualization);

        using var process = Process.Start(info) ?? throw new InvalidOperationException("The checker did not start.");
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var bound = new CancellationTokenSource(timeout + TimeSpan.FromMinutes(1));
        try
        {
            await process.WaitForExitAsync(bound.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"The checker outlived its own timeout of {timeout} by a minute.");
        }

        return new CheckerVerdict(process.ExitCode, (await output.ConfigureAwait(false)).Trim(), (await error.ConfigureAwait(false)).Trim(), File.Exists(visualization) ? visualization : null);
    }

    private static async Task<string> BuildAsync()
    {
        var given = Environment.GetEnvironmentVariable(BinaryVariable);
        if (!string.IsNullOrEmpty(given))
        {
            return File.Exists(given) ? given : throw new FileNotFoundException($"{BinaryVariable} names a file that does not exist: {given}");
        }

        var output = Path.Combine(Path.GetTempPath(), $"reeve-linearizability-checker-{Environment.ProcessId}");
        AppDomain.CurrentDomain.ProcessExit += (_, _) => File.Delete(output);
        var info = new ProcessStartInfo("go")
        {
            WorkingDirectory = Path.Combine(RepositoryRoot(), "tests", "linearizability-checker"),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in new[] { "build", "-o", output, "." })
        {
            info.ArgumentList.Add(argument);
        }

        try
        {
            using var process = Process.Start(info)!;
            var error = new StringBuilder();
            process.ErrorDataReceived += (_, e) => error.AppendLine(e.Data);
            process.BeginErrorReadLine();
            await process.WaitForExitAsync().ConfigureAwait(false);
            return process.ExitCode == 0 ? output : throw new InvalidOperationException($"go build of the checker failed: {error}");
        }
        catch (System.ComponentModel.Win32Exception exception)
        {
            throw new InvalidOperationException($"The linearizability checker needs the Go toolchain, or {BinaryVariable} naming a built checker.", exception);
        }
    }

    internal static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Reeve.Cluster.slnx")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("The repository root was not found above the test assembly.");
    }
}
