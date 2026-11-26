using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Core;
using Reeve.Cluster.Owned;

namespace Reeve.Cluster.Hosting;

internal sealed class AspNetCoreMarker
{
}

internal sealed class ClusterOptionsValidation(
    IEnumerable<AspNetCoreMarker> markers, IEnumerable<UnitKind> kinds, IEnumerable<CommandHandlerRegistration> commandHandlers) : IValidateOptions<ClusterOptions>
{
    public ValidateOptionsResult Validate(string? name, ClusterOptions options)
    {
        var registered = kinds.ToDictionary(k => k.Kind, StringComparer.Ordinal);
        UnitKind[] commandKinds =
        [
            .. commandHandlers.Select(h => registered.TryGetValue(h.Kind, out var kind) ? kind : new UnitKind(h.Kind, typeof(IOwnedUnit), new UnitKindOptions())),
        ];
        var problems = OptionsValidator.Validate(options, aspNetCoreInUse: markers.Any(), commandKinds);
        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems.Select(p => p.Message));
    }
}

internal sealed class ClusterAreaValidation(
    ClusterAreaSelection selection,
    IEnumerable<UnitKind> kinds,
    IEnumerable<AskHandlerRegistration> askHandlers,
    IEnumerable<CommandHandlerRegistration> commandHandlers,
    IEnumerable<Claims.OnceEffectRegistration> onceEffects,
    IEnumerable<PrimitivesRequested> primitives) : IValidateOptions<ClusterOptions>
{
    public ValidateOptionsResult Validate(string? name, ClusterOptions options)
    {
        List<string> problems = [];
        void Need(ClusterAreas area, IEnumerable<string> registrations)
        {
            if (!selection.Has(area))
            {
                problems.AddRange(registrations.Select(r => $"{r} is registered, but AddReeveCluster was called without ClusterAreas.{area}: add it to the areas, or remove the registration."));
            }
        }

        Need(ClusterAreas.OwnedWork, kinds.Where(k => k.Kind != Placement.PlacerKind).Select(k => $"The unit kind '{k.Kind}'"));
        Need(ClusterAreas.Ask, askHandlers.Select(h => h.Kind is null ? "The member Ask handler" : $"The Ask handler of the kind '{h.Kind}'"));
        Need(ClusterAreas.Commands, commandHandlers.Select(h => $"The durable command handler of the kind '{h.Kind}'"));
        Need(ClusterAreas.Primitives, onceEffects.Select(e => $"The once effect '{e.Name}'"));
        Need(ClusterAreas.Primitives, primitives.Select(p => $"The Redis primitive {p.Primitive}"));
        return problems.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(problems);
    }
}
