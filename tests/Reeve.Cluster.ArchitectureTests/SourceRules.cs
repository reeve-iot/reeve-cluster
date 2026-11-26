using System.Text.RegularExpressions;

namespace Arch;

public sealed class SourceRules
{
    private static readonly (string Rule, Regex Pattern)[] Banned =
    [
        ("no Redis scripts", new Regex(@"\b(ScriptEvaluate(Async)?|LuaScript|LoadedLuaScript|EVALSHA)\b|""EVAL""", RegexOptions.IgnoreCase)),
        ("no concurrent index builds", new Regex(@"\bCONCURRENTLY\b|\bIsCreatedConcurrently\b|Npgsql:CreatedConcurrently", RegexOptions.IgnoreCase)),
        ("no session-scoped advisory locks", new Regex(@"\bpg_(try_)?advisory_lock(_shared)?\s*\(|\bpg_advisory_unlock(_all|_shared)?\s*\(|\bAdvisorySessionLock\b", RegexOptions.IgnoreCase)),
    ];

    [Fact]
    public void Source_and_sql_contain_no_banned_token()
    {
        var files = Sources().ToList();
        Assert.NotEmpty(files);

        var hits = files
            .SelectMany(f => Scan(File.ReadAllText(f)).Select(rule => $"{Path.GetRelativePath(RepositoryRoot(), f)}: {rule}"))
            .ToList();

        Assert.True(hits.Count == 0, string.Join(Environment.NewLine, hits));
    }

    [Fact]
    public void EntityFramework_is_used_in_the_core_only_under_Migrations()
    {
        var core = Path.Combine(RepositoryRoot(), "src", "Reeve.Cluster");
        var migrations = Path.Combine(core, "Migrations") + Path.DirectorySeparatorChar;

        var outside = Directory.EnumerateFiles(core, "*.cs", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(f) && !f.StartsWith(migrations, StringComparison.Ordinal))
            .Where(f => UsesEntityFramework(File.ReadAllText(f)))
            .Select(f => Path.GetRelativePath(RepositoryRoot(), f))
            .ToList();

        Assert.True(outside.Count == 0, $"Microsoft.EntityFrameworkCore is used outside src/Reeve.Cluster/Migrations: {string.Join(", ", outside)}");
    }

    [Fact]
    public void The_EntityFramework_scan_finds_what_it_bans()
    {
        Assert.True(UsesEntityFramework("using Microsoft.EntityFrameworkCore;"));
        Assert.True(UsesEntityFramework("using Microsoft.EntityFrameworkCore.Migrations;"));
        Assert.True(UsesEntityFramework("var o = new Microsoft.EntityFrameworkCore.DbContextOptions();"));
        Assert.False(UsesEntityFramework("using Reeve.EntityFrameworkCore.AdvisoryLocks;"));
        Assert.False(UsesEntityFramework("// the EntityFrameworkCore adapter package"));
    }

    private static bool UsesEntityFramework(string text) => text.Contains("Microsoft.EntityFrameworkCore", StringComparison.Ordinal);

    [Fact]
    public void The_scan_finds_what_it_bans()
    {
        Assert.Contains("no Redis scripts", Scan("db.ScriptEvaluateAsync(script)"));
        Assert.Contains("no Redis scripts", Scan("redis.call('EVALSHA')"));
        Assert.Contains("no concurrent index builds", Scan("create index concurrently ix on t (c);"));
        Assert.Contains("no concurrent index builds", Scan("entity.HasIndex(x => x.Key).IsCreatedConcurrently();"));
        Assert.Contains("no concurrent index builds", Scan("""
            .Annotation("Npgsql:CreatedConcurrently", true)
            """));
        Assert.Contains("no session-scoped advisory locks", Scan("select pg_advisory_lock(1);"));
        Assert.Contains("no session-scoped advisory locks", Scan("select pg_try_advisory_lock(1);"));
        Assert.Contains("no session-scoped advisory locks", Scan("await AdvisorySessionLock.TryLockAsync(connection, key);"));
        Assert.Empty(Scan("select pg_advisory_xact_lock(1);"));
        Assert.Empty(Scan("await transaction.AdvisoryXactLockAsync(key);"));
        Assert.Empty(Scan("select pg_try_advisory_xact_lock(1);"));
    }

    private static IEnumerable<string> Scan(string text) =>
        Banned.Where(b => b.Pattern.IsMatch(text)).Select(b => b.Rule);

    private static IEnumerable<string> Sources()
    {
        var src = Path.Combine(RepositoryRoot(), "src");
        return Directory.EnumerateFiles(src, "*.*", SearchOption.AllDirectories)
            .Where(f => (f.EndsWith(".cs", StringComparison.Ordinal) || f.EndsWith(".sql", StringComparison.Ordinal))
                        && !IsBuildOutput(f));
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
