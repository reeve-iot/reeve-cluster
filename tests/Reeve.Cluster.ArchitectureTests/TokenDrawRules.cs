using System.Text.RegularExpressions;

namespace Arch;

public sealed class TokenDrawRules
{
    private static readonly Regex Sequence = new(@"\bowned_token\b", RegexOptions.IgnoreCase);

    private static readonly Regex DefinesDrawToken = new(@"\bCREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+reeve_cluster\.draw_token\s*\(", RegexOptions.IgnoreCase);

    private static readonly Regex Allowed = new(
        @"\bCREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+reeve_cluster\.draw_token\s*\(.*?\$\$.*?\$\$|\bCREATE\s+SEQUENCE\s+reeve_cluster\.owned_token\b[^;]*;"
        + @"|\bHasSequence(<\w+>)?\(\s*""owned_token""|\b(Create|Drop)Sequence(<\w+>)?\(\s*name:\s*""owned_token""",
        RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex SqlComment = new(@"--[^\n]*");

    [Fact]
    public void Only_the_definition_of_draw_token_names_the_owned_token_sequence()
    {
        var root = RepositoryRoot();
        var files = Directory.EnumerateFiles(Path.Combine(root, "src"), "*.*", SearchOption.AllDirectories)
            .Where(f => (f.EndsWith(".cs", StringComparison.Ordinal) || f.EndsWith(".sql", StringComparison.Ordinal)) && !IsBuildOutput(f))
            .ToList();

        var definitions = files.Where(f => DefinesDrawToken.IsMatch(File.ReadAllText(f))).ToList();
        Assert.NotEmpty(definitions);

        var hits = files
            .Where(f => Names(File.ReadAllText(f)))
            .Select(f => Path.GetRelativePath(root, f))
            .ToList();

        Assert.True(hits.Count == 0, "Draw tokens through reeve_cluster.draw_token(); these name owned_token: " + string.Join(", ", hits));
    }

    [Fact]
    public void The_scan_finds_what_it_bans()
    {
        Assert.True(Names("UPDATE reeve_cluster.owned SET token = nextval('reeve_cluster.owned_token')"));
        Assert.True(Names("select setval('OWNED_TOKEN', 1)"));
        Assert.False(Names("UPDATE reeve_cluster.owned SET token = reeve_cluster.draw_token()"));
        Assert.False(Names("CREATE SEQUENCE reeve_cluster.owned_token CACHE 1; CREATE FUNCTION reeve_cluster.draw_token() RETURNS bigint AS $$ BEGIN RETURN nextval('reeve_cluster.owned_token'); END $$;"));
        Assert.False(Names("modelBuilder.HasSequence<long>(\"owned_token\", \"reeve_cluster\");"));
        Assert.False(Names("migrationBuilder.CreateSequence(\n    name: \"owned_token\",\n    schema: \"reeve_cluster\");"));
        Assert.False(Names("migrationBuilder.DropSequence(\n    name: \"owned_token\",\n    schema: \"reeve_cluster\");"));
        Assert.True(Names("migrationBuilder.Sql(\"UPDATE reeve_cluster.owned SET token = nextval('reeve_cluster.owned_token')\");"));
        Assert.False(Names("-- owned_token is drawn through draw_token()\nSELECT reeve_cluster.draw_token()"));
        Assert.True(Names("CREATE OR REPLACE FUNCTION reeve_cluster.draw_token() RETURNS bigint AS $$ SELECT 1 $$; UPDATE reeve_cluster.owned SET token = nextval('reeve_cluster.owned_token')"));
    }

    private static bool Names(string text) => Sequence.IsMatch(Allowed.Replace(SqlComment.Replace(text, string.Empty), string.Empty));

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
