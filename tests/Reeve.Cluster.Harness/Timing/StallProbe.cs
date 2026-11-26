using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Reeve.Cluster.Harness.Timing;

internal sealed class StallProbe : IDisposable
{
    public static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(20);

    public static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(50);

    public static readonly TimeSpan Granularity = TimeSpan.FromMilliseconds(100);

    private static readonly Lazy<StallProbe> SharedProbe = new(() => StartNew());

    private readonly Func<long> timestamp;
    private readonly long frequency;
    private readonly object gate = new();
    private readonly List<Stall> stalls = [];
    private readonly Thread? thread;
    private long lastTick;
    private volatile bool stopped;

    internal StallProbe(Func<long> timestamp, long frequency)
    {
        this.timestamp = timestamp;
        this.frequency = frequency;
        lastTick = timestamp();
    }

    private StallProbe()
        : this(Stopwatch.GetTimestamp, Stopwatch.Frequency)
    {
        thread = new Thread(Run) { IsBackground = true, Name = "harness-stall-probe" };
        thread.Start();
    }

    public static StallProbe Shared => SharedProbe.Value;

    public static StallProbe StartNew() => new();

    public StallSpan Begin() => new(this, timestamp());

    public TimeSpan LargestGap => LargestSince(0).Length;

    public void Note(TimeSpan length, string source)
    {
        var end = timestamp();
        lock (gate)
        {
            stalls.Add(new Stall(end - (long)Math.Round(length.TotalSeconds * frequency), end, length, source));
        }
    }

    public async Task OpenNotingStallAsync(Npgsql.NpgsqlConnection connection, string source, CancellationToken cancellationToken = default)
    {
        var started = Stopwatch.StartNew();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        if (started.Elapsed > TimeSpan.FromSeconds(1))
        {
            Note(started.Elapsed, source);
        }
    }

    internal void Tick()
    {
        var now = timestamp();
        lock (gate)
        {
            var extra = ToSpan(now - lastTick) - Interval;
            if (extra >= Threshold)
            {
                stalls.Add(new Stall(lastTick, now, extra, "scheduling"));
            }

            lastTick = now;
        }
    }

    public void Dispose()
    {
        stopped = true;
        thread?.Join(TimeSpan.FromSeconds(1));
    }

    internal (TimeSpan Length, string Source) LargestSince(long start)
    {
        var now = timestamp();
        lock (gate)
        {
            var best = (Length: ToSpan(now - Math.Max(lastTick, start)) - Interval, Source: "scheduling (in progress)");
            foreach (var stall in stalls)
            {
                var overlap = ToSpan(Math.Min(stall.End, now) - Math.Max(stall.Start, start));
                if (overlap > TimeSpan.Zero)
                {
                    var length = overlap < stall.Length ? overlap : stall.Length;
                    if (length > best.Length)
                    {
                        best = (length, stall.Source);
                    }
                }
            }

            return best.Length < TimeSpan.Zero ? (TimeSpan.Zero, "none") : best;
        }
    }

    private TimeSpan ToSpan(long ticks) => TimeSpan.FromTicks((long)Math.Round(ticks * (TimeSpan.TicksPerSecond / (double)frequency)));

    private void Run()
    {
        while (!stopped)
        {
            Thread.Sleep(Interval);
            Tick();
        }
    }

    private readonly record struct Stall(long Start, long End, TimeSpan Length, string Source);
}

internal readonly struct StallSpan
{
    private readonly StallProbe probe;
    private readonly long start;

    internal StallSpan(StallProbe probe, long start)
    {
        this.probe = probe;
        this.start = start;
    }

    public TimeSpan Largest => probe.LargestSince(start).Length;

    public bool Exceeds(TimeSpan slack) => Largest > slack;

    public bool Covers(TimeSpan overrun) => overrun <= Largest + StallProbe.Granularity;

    public string Describe()
    {
        var (length, source) = probe.LargestSince(start);
        return string.Create(CultureInfo.InvariantCulture, $"largest stall {length.TotalSeconds:0.###} s ({source})");
    }
}

internal static class Degraded
{
    public const string Marker = "HARNESS-DEGRADED:";

    private static readonly object FileGate = new();

    public static string FilePath => Path.Combine(DegradedDirectory(), "degraded.txt");

    public static string Report(Action<string> log, string reason, [CallerMemberName] string member = "", [CallerFilePath] string file = "")
    {
        ArgumentNullException.ThrowIfNull(log);
        var test = $"{Path.GetFileNameWithoutExtension(file)}.{member}";
        var line = $"{Marker} {test} {reason.ReplaceLineEndings(" ")}";
        log(line);
        try
        {
            lock (FileGate)
            {
                File.AppendAllText(FilePath, line + Environment.NewLine);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
        }

        return line;
    }

    private static string DegradedDirectory()
    {
        var directory = Path.Combine(Linearizability.LinearizabilityChecker.RepositoryRoot(), "artifacts", "harness-degraded");
        Directory.CreateDirectory(directory);
        return directory;
    }
}
