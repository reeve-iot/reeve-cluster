using System.Reflection;

namespace Arch;

internal static class Packages
{
    public const string Abstractions = "Reeve.Cluster.Abstractions";
    public const string Cluster = "Reeve.Cluster";
    public const string Redis = "Reeve.Cluster.Redis";
    public const string EntityFrameworkCore = "Reeve.Cluster.EntityFrameworkCore";
    public const string AspNetCore = "Reeve.Cluster.AspNetCore";

    public static readonly string[] All = [Abstractions, Cluster, Redis, EntityFrameworkCore, AspNetCore];

    public static Assembly Load(string name) => Assembly.Load(new AssemblyName(name));

    public static IEnumerable<string> ReferencedNames(string package) =>
        Load(package).GetReferencedAssemblies().Select(a => a.Name!);
}
