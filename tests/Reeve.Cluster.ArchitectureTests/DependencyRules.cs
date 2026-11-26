namespace Arch;

public sealed class DependencyRules
{
    private static readonly string[] Redis = ["StackExchange.Redis"];
    private static readonly string[] Npgsql = ["Npgsql"];
    private static readonly string[] EntityFramework = ["Microsoft.EntityFrameworkCore"];
    private static readonly string[] AspNetCore = ["Microsoft.AspNetCore"];

    [Fact]
    public void All_five_packages_load()
    {
        foreach (var package in Packages.All)
        {
            Assert.Equal(package, Packages.Load(package).GetName().Name);
        }
    }

    [Fact]
    public void Abstractions_depends_on_nothing()
    {
        var outside = Packages.ReferencedNames(Packages.Abstractions)
            .Where(n => !IsFramework(n))
            .ToList();

        Assert.Empty(outside);
    }

    [Fact]
    public void Cluster_takes_no_Redis_or_AspNetCore()
    {
        AssertNoReference(Packages.Cluster, [.. Redis, .. AspNetCore]);
    }

    [Fact]
    public void Nothing_public_lives_in_the_namespace_of_the_migration_chain()
    {
        var exposed = Packages.Load(Packages.Cluster).GetExportedTypes()
            .Where(t => t.Namespace is "Reeve.Cluster.Migrations")
            .Select(t => t.FullName)
            .ToList();

        Assert.True(exposed.Count == 0, $"Public types in Reeve.Cluster.Migrations: {string.Join(", ", exposed)}. Make the generated migration class internal.");
    }

    [Fact]
    public void Redis_takes_no_Npgsql_EntityFramework_or_AspNetCore()
    {
        AssertNoReference(Packages.Redis, [.. Npgsql, .. EntityFramework, .. AspNetCore]);
    }

    [Fact]
    public void EntityFrameworkCore_takes_no_Redis_or_AspNetCore()
    {
        AssertNoReference(Packages.EntityFrameworkCore, [.. Redis, .. AspNetCore]);
    }

    [Fact]
    public void AspNetCore_takes_no_Redis_or_EntityFramework()
    {
        AssertNoReference(Packages.AspNetCore, [.. Redis, .. EntityFramework]);
    }

    [Fact]
    public void The_adapter_packages_do_not_reference_one_another()
    {
        AssertNoReference(Packages.Redis, [Packages.EntityFrameworkCore, Packages.AspNetCore]);
        AssertNoReference(Packages.EntityFrameworkCore, [Packages.Redis, Packages.AspNetCore]);
        AssertNoReference(Packages.AspNetCore, [Packages.Redis, Packages.EntityFrameworkCore]);
    }

    [Fact]
    public void Nothing_points_back_at_a_package_above_it()
    {
        AssertNoReference(Packages.Abstractions, [Packages.Cluster, Packages.Redis, Packages.EntityFrameworkCore, Packages.AspNetCore]);
        AssertNoReference(Packages.Cluster, [Packages.Redis, Packages.EntityFrameworkCore, Packages.AspNetCore]);
    }

    private static void AssertNoReference(string package, string[] prefixes)
    {
        var offending = Packages.ReferencedNames(package)
            .Where(n => prefixes.Any(p => n == p || n.StartsWith(p + ".", StringComparison.Ordinal)))
            .ToList();

        Assert.True(offending.Count == 0, $"{package} references {string.Join(", ", offending)}");
    }

    private static bool IsFramework(string name) =>
        name is "mscorlib" or "netstandard" || name.StartsWith("System", StringComparison.Ordinal);
}
