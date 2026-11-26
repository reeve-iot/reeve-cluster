using System.Reflection;

namespace Arch;

public sealed class ConstructorRules
{
    private const BindingFlags AllConstructors =
        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;

    [Fact]
    public void No_constructor_defaults_a_reference_parameter_to_null()
    {
        var offending = Packages.All
            .SelectMany(p => Packages.Load(p).GetTypes())
            .SelectMany(t => t.GetConstructors(AllConstructors))
            .SelectMany(c => c.GetParameters().Select(p => (Constructor: c, Parameter: p)))
            .Where(x => x.Parameter.HasDefaultValue
                        && x.Parameter.DefaultValue is null
                        && !x.Parameter.ParameterType.IsValueType)
            .Select(x => $"{x.Constructor.DeclaringType!.FullName}({x.Parameter.Name})")
            .ToList();

        Assert.True(offending.Count == 0, $"optional dependency: {string.Join(", ", offending)}");
    }
}
