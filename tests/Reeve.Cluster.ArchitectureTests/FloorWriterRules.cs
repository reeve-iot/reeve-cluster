using System.Text.RegularExpressions;

namespace Arch;

public sealed class FloorWriterRules
{
    private static readonly Regex Sets = new(
        @"\bSET\b(?:(?!\b(?:WHERE|FROM|RETURNING)\b)[^;])*?(?<![\w.])(protocol_floor|version_floor)\s*=(?!=)",
        RegexOptions.IgnoreCase);

    private static readonly Regex SqlComment = new(@"--[^\n]*");

    [Fact]
    public void Only_the_floor_store_sets_a_floor()
    {
        var hits = Directory.EnumerateFiles(Path.Combine(RepositoryRoot(), "src"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(f) && Sets.IsMatch(SqlComment.Replace(File.ReadAllText(f), string.Empty)))
            .Select(Path.GetFileName)
            .ToList();

        Assert.Equal(["FloorStore.cs"], hits);
    }

    [Fact]
    public void The_scan_finds_what_it_bans()
    {
        Assert.Matches(Sets, "UPDATE reeve_cluster.cluster\n SET protocol_floor = @protocol");
        Assert.Matches(Sets, "update reeve_cluster.cluster set Version_Floor = NULL");
        Assert.Matches(Sets, "UPDATE reeve_cluster.cluster SET floor_moved_by = @by, protocol_floor = @protocol");
        Assert.Matches(Sets, "UPDATE reeve_cluster.cluster\n SET floor_moved_at = now(),\n     version_floor = NULL\n WHERE id = 1");
        Assert.DoesNotMatch(Sets, "UPDATE reeve_cluster.member SET state = 'active' WHERE protocol_floor = 0");
        Assert.DoesNotMatch(Sets, "UPDATE reeve_cluster.member SET state = 'active' FROM reeve_cluster.cluster c WHERE c.version_floor = NULL");
        Assert.DoesNotMatch(Sets, "SELECT protocol_floor, version_floor FROM reeve_cluster.cluster");
        Assert.DoesNotMatch(Sets, SqlComment.Replace("-- SET protocol_floor = 0 is the harness's\nSELECT 1", string.Empty));
    }

    private static bool IsBuildOutput(string path) =>
        path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
        || path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal);

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
