using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Reeve.Cluster.EntityFrameworkCore;

internal static class EntryCapture
{
    public static List<ChangeEntry> Entries(DbContext context, IClusterMapping mapping)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(mapping);

        var tracked = context.ChangeTracker.Entries().ToList();
        IReadOnlyDictionary<IKey, PrincipalIndex>? principalIndexes = null;
        IReadOnlyDictionary<IKey, PrincipalIndex> PrincipalIndexes()
            => principalIndexes ??= BuildPrincipalIndexes(tracked);
        var entries = new List<ChangeEntry>();
        var cascadeEntries = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<IEntityType>();
        foreach (var entry in tracked)
        {
            if (entry.State is not (EntityState.Added or EntityState.Modified or EntityState.Deleted))
            {
                continue;
            }

            if (mapping.Find(entry.Metadata.ClrType) is { } map)
            {
                AddMappedEntry(entry, map, entries);
            }

            if (entry.Metadata.IsOwned() && mapping.Find(entry.Metadata.ClrType) is null)
            {
                var rootType = OwnershipRoot(entry.Metadata);
                var rootEntry = ResolveOwnershipRoot(entry, PrincipalIndexes(), rootType);
                if (rootEntry is not null && mapping.Find(rootEntry.Metadata.ClrType) is { } rootMap)
                {
                    entries.Add(new ChangeEntry(rootMap.AggregateType, rootMap.Key?.Invoke(rootEntry.Entity)));
                }
                else
                {
                    foreach (var fallbackMap in MappedAggregates(rootType, mapping))
                    {
                        entries.Add(new ChangeEntry(fallbackMap.AggregateType, null));
                    }
                }
            }

            foreach (var foreignKey in entry.Metadata.GetForeignKeys())
            {
                if (!foreignKey.GetReferencingSkipNavigations().Any())
                {
                    continue;
                }

                AddPrincipalEntries(entry, foreignKey, PrincipalIndexes(), foreignKey.Properties.Any(p => entry.Property(p.Name).IsModified), mapping, entries);
            }

            if (entry.State == EntityState.Deleted)
            {
                Cascade(entry.Metadata, mapping, entries, cascadeEntries, visited);
            }
        }

        return entries;
    }

    private static void AddMappedEntry(EntityEntry entry, EntityMapping map, List<ChangeEntry> entries)
    {
        var key = map.Key?.Invoke(entry.Entity);
        string? original = null;
        var keyChanged = false;
        if (entry.State != EntityState.Added && map.Key is { } keyOf)
        {
            original = keyOf(entry.OriginalValues.ToObject());
            keyChanged = original != key;
        }

        if (entry.State == EntityState.Modified && !keyChanged && !CountedChange(entry, map))
        {
            return;
        }

        entries.Add(new ChangeEntry(map.AggregateType, key));
        if (keyChanged)
        {
            entries.Add(new ChangeEntry(map.AggregateType, original));
        }
    }

    private static EntityEntry? ResolveOwnershipRoot(EntityEntry owned, IReadOnlyDictionary<IKey, PrincipalIndex> indexes, IEntityType rootType)
    {
        var current = owned;
        while (current.Metadata.IsOwned())
        {
            var ownership = current.Metadata.FindOwnership()!;
            var principal = ResolvePrincipal(current, ownership, indexes, originalValues: false);
            if (principal is null)
            {
                return null;
            }

            current = principal;
        }

        return rootType.IsAssignableFrom(current.Metadata) ? current : null;
    }

    private static IEntityType OwnershipRoot(IEntityType type)
    {
        while (type.IsOwned())
        {
            type = type.FindOwnership()!.PrincipalEntityType;
        }

        return type;
    }

    private static Dictionary<IKey, PrincipalIndex> BuildPrincipalIndexes(IReadOnlyList<EntityEntry> tracked)
    {
        var indexes = new Dictionary<IKey, PrincipalIndex>();
        foreach (var entry in tracked)
        {
            foreach (var key in entry.Metadata.GetKeys())
            {
                if (!indexes.TryGetValue(key, out var index))
                {
                    indexes.Add(key, index = new PrincipalIndex(key.Properties));
                }

                index.Add(entry, key.Properties.Select(p => entry.Property(p.Name).CurrentValue).ToArray());
            }
        }

        return indexes;
    }

    private static void AddPrincipalEntries(
        EntityEntry dependent,
        IForeignKey foreignKey,
        IReadOnlyDictionary<IKey, PrincipalIndex> indexes,
        bool includeOriginal,
        IClusterMapping mapping,
        List<ChangeEntry> entries)
    {
        AddPrincipalEntry(dependent, foreignKey, indexes, originalValues: false, mapping, entries);
        if (includeOriginal)
        {
            AddPrincipalEntry(dependent, foreignKey, indexes, originalValues: true, mapping, entries);
        }
    }

    private static void AddPrincipalEntry(
        EntityEntry dependent,
        IForeignKey foreignKey,
        IReadOnlyDictionary<IKey, PrincipalIndex> indexes,
        bool originalValues,
        IClusterMapping mapping,
        List<ChangeEntry> entries)
    {
        var principal = ResolvePrincipal(dependent, foreignKey, indexes, originalValues);
        if (principal is not null && mapping.Find(principal.Metadata.ClrType) is { } principalMap)
        {
            entries.Add(new ChangeEntry(principalMap.AggregateType, principalMap.Key?.Invoke(principal.Entity)));
            return;
        }

        foreach (var fallbackMap in MappedAggregates(foreignKey.PrincipalEntityType, mapping))
        {
            entries.Add(new ChangeEntry(fallbackMap.AggregateType, null));
        }
    }

    private static EntityEntry? ResolvePrincipal(
        EntityEntry dependent,
        IForeignKey foreignKey,
        IReadOnlyDictionary<IKey, PrincipalIndex> indexes,
        bool originalValues)
    {
        if (!indexes.TryGetValue(foreignKey.PrincipalKey, out var index))
        {
            return null;
        }

        var values = foreignKey.Properties.Select(property =>
        {
            var dependentProperty = dependent.Property(property.Name);
            return originalValues || dependent.State == EntityState.Deleted
                ? dependentProperty.OriginalValue
                : dependentProperty.CurrentValue;
        }).ToArray();

        return index.Find(values, foreignKey.PrincipalEntityType);
    }

    private static IEnumerable<EntityMapping> MappedAggregates(IEntityType type, IClusterMapping mapping)
    {
        var found = new Dictionary<string, EntityMapping>(StringComparer.Ordinal);
        var pending = new Stack<IEntityType>();
        var visited = new HashSet<IEntityType>();
        pending.Push(type);
        while (pending.TryPop(out var current))
        {
            if (!visited.Add(current))
            {
                continue;
            }

            foreach (var derived in current.GetDerivedTypesInclusive())
            {
                if (mapping.Find(derived.ClrType) is { } map)
                {
                    found.TryAdd(map.AggregateType, map);
                }

                if (derived.IsOwned())
                {
                    pending.Push(derived.FindOwnership()!.PrincipalEntityType);
                }

                foreach (var foreignKey in derived.GetForeignKeys().Where(fk => fk.GetReferencingSkipNavigations().Any()))
                {
                    pending.Push(foreignKey.PrincipalEntityType);
                }
            }
        }

        return found.Values;
    }

    private sealed class PrincipalIndex(IReadOnlyList<IProperty> properties)
    {
        private readonly Dictionary<object?[], List<EntityEntry>> byKey = new(new KeyValuesComparer(properties));

        public void Add(EntityEntry entry, object?[] values)
        {
            if (!byKey.TryGetValue(values, out var matches))
            {
                byKey.Add(values, matches = []);
            }

            matches.Add(entry);
        }

        public EntityEntry? Find(object?[] values, IEntityType principalType)
            => byKey.TryGetValue(values, out var matches)
                ? matches.FirstOrDefault(entry => principalType.IsAssignableFrom(entry.Metadata))
                : null;
    }

    private sealed class KeyValuesComparer(IReadOnlyList<IProperty> properties) : IEqualityComparer<object?[]>
    {
        public bool Equals(object?[]? left, object?[]? right)
        {
            if (ReferenceEquals(left, right)) return true;
            if (left is null || right is null || left.Length != right.Length) return false;
            for (var i = 0; i < properties.Count; i++)
            {
                if (!properties[i].GetKeyValueComparer().Equals(left[i], right[i])) return false;
            }

            return true;
        }

        public int GetHashCode(object?[] values)
        {
            var hash = new HashCode();
            for (var i = 0; i < properties.Count; i++)
            {
                hash.Add(properties[i].GetKeyValueComparer().GetHashCode(values[i]));
            }

            return hash.ToHashCode();
        }
    }

    private static void Cascade(
        IEntityType principalType,
        IClusterMapping mapping,
        List<ChangeEntry> entries,
        HashSet<string> cascadeEntries,
        HashSet<IEntityType> visited)
    {
        if (!visited.Add(principalType))
        {
            return;
        }

        foreach (var foreignKey in principalType.GetReferencingForeignKeys())
        {
            if (foreignKey.IsOwnership)
            {
                continue;
            }

            if (foreignKey.DeleteBehavior == DeleteBehavior.Cascade)
            {
                foreach (var dependentType in foreignKey.DeclaringEntityType.GetDerivedTypesInclusive())
                {
                    foreach (var dependentMap in MappedAggregates(dependentType, mapping))
                    {
                        if (cascadeEntries.Add(dependentMap.AggregateType))
                        {
                            entries.Add(new ChangeEntry(dependentMap.AggregateType, null));
                        }
                    }

                    Cascade(dependentType, mapping, entries, cascadeEntries, visited);
                }
            }
            else if (foreignKey.DeleteBehavior == DeleteBehavior.SetNull)
            {
                foreach (var dependentMap in MappedAggregates(foreignKey.DeclaringEntityType, mapping))
                {
                    var ownedChange = foreignKey.DeclaringEntityType.IsOwned();
                    if ((ownedChange || dependentMap.CountedProperties is null || foreignKey.Properties.Any(p => dependentMap.CountedProperties.Contains(p.Name)))
                        && cascadeEntries.Add(dependentMap.AggregateType))
                    {
                        entries.Add(new ChangeEntry(dependentMap.AggregateType, null));
                    }
                }
            }
        }
    }

    internal static bool IsCounted(EntityMapping map, string name)
        => map.CountedProperties is not { } counted || counted.Contains(name);

    private static bool CountedChange(EntityEntry entry, EntityMapping map)
    {
        if (map.CountedProperties is not { } counted)
        {
            return true;
        }

        foreach (var name in counted)
        {
            if (entry.Metadata.FindProperty(name) is null && entry.Metadata.FindComplexProperty(name) is null)
            {
                throw new InvalidOperationException($"{map.EntityType.Name} counts the column '{name}', which is not a property or complex property of the entity in the model.");
            }
        }

        return entry.Properties.Any(p => p.IsModified && IsCounted(map, p.Metadata.Name))
               || counted.Any(name => entry.Metadata.FindComplexProperty(name) is { } complex
                   && (complex.IsCollection ? ComplexChanged(entry.ComplexCollection(name)) : ComplexChanged(entry.ComplexProperty(name))));
    }

    private static bool ComplexChanged(ComplexPropertyEntry complex)
        => complex.Properties.Any(p => p.IsModified)
           || complex.ComplexProperties.Any(ComplexChanged)
           || complex.ComplexCollections.Any(ComplexChanged);

    private static bool ComplexChanged(ComplexCollectionEntry complex)
    {
        if (complex.IsModified)
        {
            return true;
        }

        using var elements = complex.GetEnumerator();
        while (elements.MoveNext())
        {
            if (ComplexChanged(elements.Current))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ComplexChanged(ComplexElementEntry complex)
        => complex.Properties.Any(p => p.IsModified)
           || complex.ComplexProperties.Any(ComplexChanged)
           || complex.ComplexCollections.Any(ComplexChanged);
}
