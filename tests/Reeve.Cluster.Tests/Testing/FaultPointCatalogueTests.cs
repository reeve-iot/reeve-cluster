using System.Reflection;
using Reeve.Cluster;

namespace Unit.Testing;

public sealed class FaultPointCatalogueTests
{
    [Fact]
    public void Every_entry_of_every_area_is_in_the_catalogue_and_every_area_is_listed()
    {
        var declared = typeof(FaultPointCatalogue).Assembly.GetTypes()
            .Where(t => t.Name.EndsWith("FaultPoints", StringComparison.Ordinal) && t.IsAbstract && t.IsSealed)
            .ToList();

        Assert.Equal(
            declared.Select(t => t.Name).Order(),
            FaultPointCatalogue.Areas.Select(t => t.Name).Order());

        var fields = declared
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static))
            .Where(f => f.FieldType == typeof(FaultPoint))
            .ToList();

        Assert.Equal(fields.Count, FaultPointCatalogue.All.Count);
        Assert.NotEmpty(FaultPointCatalogue.All);
    }

    [Fact]
    public void Names_are_unique_and_carry_their_areas_prefix()
    {
        var names = FaultPointCatalogue.All.Select(p => p.Name).ToList();

        Assert.Equal(names.Count, names.Distinct(StringComparer.Ordinal).Count());
        Assert.All(names, n => Assert.Matches(@"^[a-z]+(\.[a-z0-9]+(-[a-z0-9]+)*)+$", n));

        foreach (var area in FaultPointCatalogue.Areas)
        {
            var prefix = area.Name.Replace("FaultPoints", string.Empty, StringComparison.Ordinal).ToLowerInvariant() + ".";
            foreach (var field in area.GetFields(BindingFlags.Public | BindingFlags.Static).Where(f => f.FieldType == typeof(FaultPoint)))
            {
                var point = (FaultPoint)field.GetValue(null)!;
                Assert.StartsWith(prefix, point.Name, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void Every_entry_names_an_action_of_the_vocabulary_or_says_it_has_none()
    {
        var vocabulary = Vocabulary().Values.ToHashSet(StringComparer.Ordinal);

        foreach (var point in FaultPointCatalogue.All)
        {
            if (point.Action is not null)
            {
                Assert.Contains(point.Action, vocabulary);
            }
        }

        Assert.Contains(FaultPointCatalogue.All, p => p.Action is null);
    }

    [Fact]
    public void The_events_that_are_not_actions_of_Next_are_left_out_of_the_vocabulary()
    {
        var vocabulary = Vocabulary().Values.ToHashSet(StringComparer.Ordinal);

        Assert.DoesNotContain("Step0", vocabulary);
        Assert.DoesNotContain("CaughtUp", vocabulary);
        Assert.DoesNotContain("RepairEntry", vocabulary);
    }

    [Fact]
    public void An_action_is_named_after_its_field()
    {
        foreach (var (field, action) in Vocabulary())
        {
            Assert.Equal(field.Split('.')[^1], action);
        }
    }

    [Fact]
    public void Find_resolves_a_name_the_harness_holds()
    {
        Assert.Equal(MembershipFaultPoints.RenewSend, FaultPointCatalogue.Find("membership.renew.send"));
        Assert.Null(FaultPointCatalogue.Find("membership.renew.sent"));
    }

    [Fact]
    public async Task Reaching_a_catalogue_entry_never_holds_in_production()
    {
        var reached = NoFaultPoints.Instance.ReachAsync(LogFaultPoints.AppendBefore);

        Assert.True(reached.IsCompletedSuccessfully);
        await reached;
    }

    private static Dictionary<string, string> Vocabulary() =>
        new[] { typeof(MembershipActions), typeof(LogActions), typeof(OwnedActions) }
            .SelectMany(t => t.GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(f => f.IsLiteral)
                .Select(f => (Key: $"{t.Name}.{f.Name}", Value: (string)f.GetRawConstantValue()!)))
            .ToDictionary(x => x.Key, x => x.Value);
}
