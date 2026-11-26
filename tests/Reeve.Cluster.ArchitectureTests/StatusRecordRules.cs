using System.Text.RegularExpressions;

namespace Arch;

public sealed class StatusRecordRules
{
    private static readonly Regex Reads = new(@"\b(FROM|JOIN)\s+(reeve_cluster\.)?fence_history\b", RegexOptions.IgnoreCase);

    private static readonly Regex MemberStatusReads = new(@"\b(FROM|JOIN)\s+(reeve_cluster\.)?member_status\b", RegexOptions.IgnoreCase);

    private static readonly Regex SqlComment = new(@"--[^\n]*");

    private static readonly (string File, string Statement)[] Allowed = [("FenceStore.cs", "RecordSql"), ("StatusStore.cs", "FencesSql")];

    [Fact]
    public void Only_the_fence_writers_trim_and_the_status_read_select_from_fence_history()
    {
        var root = RepositoryRoot();
        var files = Directory.EnumerateFiles(Path.Combine(root, "src"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(f))
            .ToList();

        var hits = files.Where(f => Selects(File.ReadAllText(f))).Select(Path.GetFileName).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.Equal(Allowed.Select(a => a.File).Order(StringComparer.Ordinal), hits);

        foreach (var (file, statement) in Allowed)
        {
            var text = File.ReadAllText(files.Single(f => Path.GetFileName(f) == file));
            Assert.True(Selects(Statement(text, statement)), $"{file}: {statement} no longer selects from fence_history");
            Assert.False(Selects(WithoutStatement(text, statement)), $"{file} selects from fence_history outside {statement}");
        }
    }

    [Fact]
    public void Only_the_status_store_selects_from_member_status()
    {
        var hits = Sources().Where(f => MemberStatusReads.IsMatch(SqlComment.Replace(File.ReadAllText(f), string.Empty)))
            .Select(Path.GetFileName).Distinct().ToList();

        Assert.Equal(["StatusStore.cs"], hits);
    }

    [Fact]
    public void The_scan_finds_what_it_bans()
    {
        Assert.Matches(MemberStatusReads, "SELECT payload FROM reeve_cluster.member_status WHERE replica_id = @id");
        Assert.Matches(MemberStatusReads, "DELETE FROM reeve_cluster.member_status s");
        Assert.DoesNotMatch(MemberStatusReads, "INSERT INTO reeve_cluster.member_status (replica_id) VALUES (@id)");

        Assert.True(Selects("SELECT trigger FROM reeve_cluster.fence_history WHERE replica_id = @id"));
        Assert.True(Selects("select 1 from\n  Reeve_Cluster.fence_history"));
        Assert.True(Selects("SELECT 1 FROM reeve_cluster.member m JOIN reeve_cluster.fence_history f ON f.replica_id = m.replica_id"));
        Assert.True(Selects("SELECT 1 FROM fence_history"));
        Assert.False(Selects("INSERT INTO reeve_cluster.fence_history (replica_id) VALUES (@id)"));
        Assert.False(Selects("-- FROM reeve_cluster.fence_history is the status read's\nSELECT 1"));

        const string Text = "a const string One = \"\"\"\n SELECT 1 FROM reeve_cluster.fence_history\n \"\"\";\n const string Two = \"\"\"\n SELECT 2\n \"\"\";";
        Assert.True(Selects(Statement(Text, "One")));
        Assert.False(Selects(WithoutStatement(Text, "One")));
        Assert.True(Selects(WithoutStatement(Text, "Two")));
    }

    private static IEnumerable<string> Sources() =>
        Directory.EnumerateFiles(Path.Combine(RepositoryRoot(), "src"), "*.cs", SearchOption.AllDirectories).Where(f => !IsBuildOutput(f));

    private static bool Selects(string text) => Reads.IsMatch(SqlComment.Replace(text, string.Empty));

    private static Regex StatementBlock(string name) => new(@"\b" + name + @"\s*=\s*""""""(.*?)""""""", RegexOptions.Singleline);

    private static string Statement(string text, string name) => StatementBlock(name).Match(text).Value;

    private static string WithoutStatement(string text, string name) => StatementBlock(name).Replace(text, string.Empty);

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
