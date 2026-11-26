using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace Reeve.Cluster.Harness.Processes;

internal static class LoopbackPorts
{
    private const string EphemeralRange = "/proc/sys/net/ipv4/ip_local_port_range";
    private const int FirstUnprivileged = 1024;
    private const int LastPort = 65535;

    private static readonly object Sync = new();
    private static int? next;

    public static Dictionary<string, int> Reserve(IReadOnlyCollection<string> ids)
    {
        var (first, count) = OutsideEphemeralRange();
        if (count < ids.Count)
        {
            throw new InvalidOperationException(
                $"{EphemeralRange} leaves {count} ports between {FirstUnprivileged} and {LastPort} outside it and the cluster needs {ids.Count}; narrow the range so that they fit below or above it.");
        }

        lock (Sync)
        {
            var cursor = (next ?? Environment.ProcessId) % count;
            var ports = new Dictionary<string, int>(StringComparer.Ordinal);
            var tried = 0;
            foreach (var id in ids)
            {
                while (true)
                {
                    if (tried == count)
                    {
                        throw new InvalidOperationException($"Only {ports.Count} of the {ids.Count} loopback ports the cluster needs are free in {first}..{first + count - 1}.");
                    }

                    var port = first + cursor;
                    cursor = (cursor + 1) % count;
                    tried++;
                    if (IsFree(port))
                    {
                        ports.Add(id, port);
                        break;
                    }
                }
            }

            next = cursor;
            return ports;
        }
    }

    private static bool IsFree(int port)
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            probe.Bind(new IPEndPoint(IPAddress.Loopback, port));
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static (int First, int Count) OutsideEphemeralRange()
    {
        var bounds = File.ReadAllText(EphemeralRange)
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
            .Select(bound => int.Parse(bound, CultureInfo.InvariantCulture))
            .ToArray();
        var below = Math.Max(0, bounds[0] - FirstUnprivileged);
        var above = Math.Max(0, LastPort - bounds[1]);
        return below >= above ? (FirstUnprivileged, below) : (bounds[1] + 1, above);
    }
}
