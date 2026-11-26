using System.Reflection;
using System.Text.RegularExpressions;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Stimuli;

namespace Arch;

public sealed class StimulusTableRules
{
    private static readonly (string Row, string PointAction)[] SharedPoints =
    [
        ("AttemptHang", "AttemptOK"),
        ("LateReturn", "AttemptOK"),
        ("RenewZeroL", "RenewAnswer"),
        ("AckDone", "AckRecord"),
    ];

    private static readonly Dictionary<HarnessOperation, string> Owner = new()
    {
        [HarnessOperation.PauseReplica] = "Pause",
        [HarnessOperation.ResumeReplica] = "Resume",
        [HarnessOperation.CrashReplica] = "Crash",
        [HarnessOperation.RestartReplica] = "Restart",
        [HarnessOperation.StopReplica] = "Drain1",
        [HarnessOperation.StopReplicaEarly] = "StopEarly",
        [HarnessOperation.OperatorDelete] = "OperatorDelete",
        [HarnessOperation.RaiseProtocolFloor] = "FloorRaise",
        [HarnessOperation.RestoreDatabase] = "Restore",
        [HarnessOperation.ReconcilerThrow] = "AttemptFail",
        [HarnessOperation.ReconcilerHang] = "AttemptHang",
        [HarnessOperation.ReconcilerRelease] = "LateReturn",
        [HarnessOperation.FixReconcilerFault] = "FixFault",
        [HarnessOperation.ReportFatalFault] = "FenceAny",
        [HarnessOperation.ReportFatalFaultWhileFenced] = "FaultFenced",
        [HarnessOperation.StartWrite] = "WBegin",
        [HarnessOperation.SaveInForeignTransaction] = "WSaveRefuse",
        [HarnessOperation.RollbackWrite] = "WRollback",
        [HarnessOperation.AcknowledgeAtCommit] = "AckAtCommit",
        [HarnessOperation.StartRead] = "QStart",
    };

    [Fact]
    public void The_table_names_only_catalogue_entries()
    {
        var catalogue = FaultPointCatalogue.All.ToHashSet();

        var unknown = StimulusTable.All
            .SelectMany(r => r.Points.Select(p => (r.Action, p.Point)))
            .Where(x => !catalogue.Contains(x.Point))
            .Select(x => $"{x.Action}: {x.Point.Name}")
            .ToList();

        Assert.Empty(unknown);
        Assert.Contains(StimulusTable.All, r => r.Points.Count > 0);
    }

    [Fact]
    public void A_point_of_a_row_sits_next_to_the_rows_action_or_to_no_action()
    {
        var wrong = StimulusTable.All
            .SelectMany(r => r.Points.Select(p => (Row: r.Action, p.Point)))
            .Where(x => x.Point.Action is not null
                        && x.Point.Action != x.Row
                        && !SharedPoints.Contains((x.Row, x.Point.Action)))
            .Select(x => $"{x.Row}: {x.Point.Name} sits next to {x.Point.Action}")
            .ToList();

        Assert.Empty(wrong);
    }

    [Fact]
    public void Every_action_the_code_may_emit_has_a_row()
    {
        var vocabulary = new[] { typeof(MembershipActions), typeof(LogActions) }
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static).Where(f => f.IsLiteral).Select(f => (string)f.GetRawConstantValue()!))
            .ToList();

        Assert.DoesNotContain(vocabulary, a => StimulusTable.Find(a) is null);
    }

    [Fact]
    public void Every_point_of_the_catalogue_that_sits_next_to_an_action_is_in_a_row_of_the_table()
    {
        var inTable = StimulusTable.All.SelectMany(r => r.Points).Select(p => p.Point).ToHashSet();

        var orphans = FaultPointCatalogue.All.Where(p => !inTable.Contains(p) && p.Action is not null).Select(p => p.Name).ToList();

        Assert.Empty(orphans);
    }

    [Fact]
    public void An_operation_is_in_the_row_of_the_action_it_performs_and_in_no_other()
    {
        var withOperation = StimulusTable.All.Where(r => r.Operation is not null).ToList();

        var wrong = withOperation
            .Where(r => !Owner.TryGetValue(r.Operation!.Value, out var owner) || owner != r.Action)
            .Select(r => $"{r.Action} performs {r.Operation}, which is {(Owner.TryGetValue(r.Operation!.Value, out var o) ? o + "'s" : "owned by no action in the rule's map")}")
            .ToList();
        var shared = withOperation
            .GroupBy(r => r.Operation!.Value)
            .Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} is in {string.Join(", ", g.Select(r => r.Action))}")
            .ToList();

        Assert.Empty(wrong);
        Assert.Empty(shared);
    }

    [Fact]
    public void A_row_of_a_library_action_that_has_no_point_is_pending_or_names_the_test_that_proves_it()
    {
        var vocabulary = new[] { typeof(MembershipActions), typeof(LogActions) }
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static).Where(f => f.IsLiteral).Select(f => (string)f.GetRawConstantValue()!))
            .ToHashSet(StringComparer.Ordinal);
        var tests = TestSource();

        var problems = new List<string>();
        foreach (var row in StimulusTable.All.Where(r => vocabulary.Contains(r.Action) && r.Points.Count == 0))
        {
            if (row.PlacedBy is null == (row.ProvenBy is null))
            {
                problems.Add($"{row.Action}: it must name the task that builds it or the test that proves it, and not both");
            }
            else if (row.ProvenBy is not null && !Regex.IsMatch(tests, $@"\b{Regex.Escape(row.ProvenBy)}\s*\("))
            {
                problems.Add($"{row.Action}: no test named {row.ProvenBy}");
            }
        }

        Assert.Empty(problems);
        Assert.Contains(StimulusTable.All, r => r.Points.Count == 0 && vocabulary.Contains(r.Action));
    }

    private static string TestSource()
    {
        var tests = Path.Combine(RepositoryRoot(), "tests");
        return string.Join(
            '\n',
            Directory.EnumerateFiles(tests, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Replace('\\', '/').Contains("/obj/", StringComparison.Ordinal) && !f.Replace('\\', '/').Contains("/Stimuli/", StringComparison.Ordinal))
                .Select(f => Regex.Replace(File.ReadAllText(f), @"/\*.*?\*/|//[^\r\n]*", string.Empty, RegexOptions.Singleline)));
    }

    [Fact]
    public void A_row_has_what_its_kind_needs()
    {
        var problems = new List<string>();
        foreach (var row in StimulusTable.All)
        {
            if (row.Kind == StimulusKind.Point && row.Points.Count == 0 && row.PlacedBy is null)
            {
                problems.Add($"{row.Action}: a point row with no point must name the task that adds it");
            }

            if (row.Kind == StimulusKind.Operation && row.Operation is null)
            {
                problems.Add($"{row.Action}: an operation row without an operation");
            }

            if (row.Kind == StimulusKind.Elapse && row.Advance == TimeAdvance.None)
            {
                problems.Add($"{row.Action}: a row of time says no clock");
            }

            if (row.Kind is StimulusKind.Operation or StimulusKind.Follows && row.Points.Count > 0)
            {
                problems.Add($"{row.Action}: an {row.Kind} row stands at no point");
            }
        }

        Assert.Empty(problems);
    }

    [Fact]
    public void An_entry_that_waits_for_a_task_names_a_task()
    {
        var unnamed = StimulusTable.All
            .SelectMany(r => r.Points.Select(p => p.PlacedBy).Append(r.PlacedBy))
            .Where(t => t is not null && !Regex.IsMatch(t, @"^\d+\.\d+$"))
            .ToList();

        Assert.Empty(unnamed);
    }

    [Fact]
    public void An_entry_that_waits_for_no_task_is_backed_by_code()
    {
        var referenced = LibrarySource();
        var problems = new List<string>();

        foreach (var row in StimulusTable.All)
        {
            foreach (var entry in row.Points.Where(p => p.PlacedBy is null))
            {
                var field = FieldNameOf(entry.Point);
                if (!referenced.Contains(field))
                {
                    problems.Add($"{row.Action}: {entry.Point.Name} names no task, but the library never reaches {field}");
                }
            }

            if (row.PlacedBy is null && row.Operation is { } op && !HarnessOperations.Implemented.Contains(op))
            {
                problems.Add($"{row.Action}: {op} names no task, but the harness has no method for it");
            }
        }

        Assert.Empty(problems);
    }

    [Fact]
    public void The_scan_of_library_source_finds_a_point_it_should_and_skips_comments()
    {
        Assert.Contains("MembershipFaultPoints.X", Scan("await points.ReachAsync(MembershipFaultPoints.X)"));
        Assert.Empty(Scan("// MembershipFaultPoints.X is placed by 1.4"));
        Assert.Empty(Scan("/// <see cref=\"MembershipFaultPoints.X\"/>"));
        Assert.Empty(Scan("/* MembershipFaultPoints.X */ var y = 1;"));
        Assert.Empty(Scan("// no point here"));
    }

    private static string FieldNameOf(FaultPoint point)
    {
        foreach (var area in FaultPointCatalogue.Areas)
        {
            var field = area.GetFields(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(f => f.FieldType == typeof(FaultPoint) && ((FaultPoint)f.GetValue(null)!).Equals(point));
            if (field is not null)
            {
                return $"{area.Name}.{field.Name}";
            }
        }

        throw new InvalidOperationException($"{point.Name} is in no area of the catalogue.");
    }

    private static HashSet<string> Scan(string text)
    {
        var code = Regex.Replace(text, @"/\*.*?\*/|//[^\r\n]*", string.Empty, RegexOptions.Singleline);
        return Regex.Matches(code, @"\b([A-Za-z]+FaultPoints\.[A-Za-z0-9]+)\b").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
    }

    private static HashSet<string> LibrarySource()
    {
        var src = Path.Combine(RepositoryRoot(), "src");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(src, "*.cs", SearchOption.AllDirectories))
        {
            var normalised = file.Replace('\\', '/');
            if (normalised.Contains("/obj/", StringComparison.Ordinal)
                || normalised.Contains("/bin/", StringComparison.Ordinal)
                || normalised.EndsWith("/Testing/FaultPoints.cs", StringComparison.Ordinal))
            {
                continue;
            }

            names.UnionWith(Scan(File.ReadAllText(file)));
        }

        return names;
    }

    private static string RepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Reeve.Cluster.slnx")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Reeve.Cluster.slnx not found above " + AppContext.BaseDirectory);
    }
}
