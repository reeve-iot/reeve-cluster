using System.Linq.Expressions;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed class ClusterMappingBuilder : IClusterMappingBuilder
{
    private readonly List<IPendingMapping> entities = [];

    public IEntityMappingBuilder<TEntity> Entity<TEntity>(string aggregateType)
        where TEntity : class
    {
        if (string.IsNullOrWhiteSpace(aggregateType))
        {
            throw new ArgumentException("The aggregate type is not empty.", nameof(aggregateType));
        }

        if (entities.Any(e => e.EntityType == typeof(TEntity)))
        {
            throw new InvalidOperationException($"{typeof(TEntity).Name} is already mapped.");
        }

        var builder = new EntityMappingBuilder<TEntity>(aggregateType);
        entities.Add(builder);
        return builder;
    }

    public ClusterMapping Build() => new([.. entities.Select(e => e.Build())]);

    private interface IPendingMapping
    {
        Type EntityType { get; }

        EntityMapping Build();
    }

    private sealed class EntityMappingBuilder<TEntity>(string aggregateType) : IEntityMappingBuilder<TEntity>, IPendingMapping
        where TEntity : class
    {
        private Func<object, string>? key;
        private bool noKey;
        private HashSet<string>? columns;
        private bool allColumns;

        public Type EntityType => typeof(TEntity);

        public IEntityMappingBuilder<TEntity> Key(Func<TEntity, string> keySelector)
        {
            ArgumentNullException.ThrowIfNull(keySelector);
            ChooseKey();
            key = entity => keySelector((TEntity)entity);
            return this;
        }

        public IEntityMappingBuilder<TEntity> NoKey()
        {
            ChooseKey();
            noKey = true;
            return this;
        }

        public IEntityMappingBuilder<TEntity> Columns(params Expression<Func<TEntity, object?>>[] properties)
        {
            ArgumentNullException.ThrowIfNull(properties);
            return Columns([.. properties.Select(NameOf)]);
        }

        public IEntityMappingBuilder<TEntity> Columns(params string[] propertyNames)
        {
            ArgumentNullException.ThrowIfNull(propertyNames);
            if (propertyNames.Length == 0 || propertyNames.Any(string.IsNullOrWhiteSpace))
            {
                throw new ArgumentException($"{typeof(TEntity).Name}: Columns names at least one property, each by a name.", nameof(propertyNames));
            }

            ChooseColumns();
            columns = [.. propertyNames];
            return this;
        }

        public IEntityMappingBuilder<TEntity> AllColumns()
        {
            ChooseColumns();
            allColumns = true;
            return this;
        }

        public EntityMapping Build()
        {
            if (key is null && !noKey)
            {
                throw new InvalidOperationException($"{typeof(TEntity).Name} is mapped without a key: call Key(...), or NoKey() when its reconciler re-reads the whole type.");
            }

            if (columns is null && !allColumns)
            {
                throw new InvalidOperationException($"{typeof(TEntity).Name} is mapped without columns: call Columns(...) to name the ones that count, or AllColumns().");
            }

            return new EntityMapping(typeof(TEntity), aggregateType, key, columns);
        }

        private void ChooseKey()
        {
            if (key is not null || noKey)
            {
                throw new InvalidOperationException($"{typeof(TEntity).Name}: the key is chosen once, with Key(...) or NoKey().");
            }
        }

        private void ChooseColumns()
        {
            if (columns is not null || allColumns)
            {
                throw new InvalidOperationException($"{typeof(TEntity).Name}: the counted columns are chosen once, with Columns(...) or AllColumns().");
            }
        }

        private static string NameOf(Expression<Func<TEntity, object?>> property)
        {
            ArgumentNullException.ThrowIfNull(property);
            var body = property.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert ? convert.Operand : property.Body;
            return body is MemberExpression { Member: var member } m && m.Expression is ParameterExpression
                ? member.Name
                : throw new ArgumentException($"{typeof(TEntity).Name}: Columns takes a property of the entity, as e => e.Name; a shadow property goes by name.", nameof(property));
        }
    }
}

internal sealed class ClusterMapping : IClusterMapping
{
    private readonly Dictionary<Type, EntityMapping> byType;

    public ClusterMapping(IReadOnlyList<EntityMapping> entities)
    {
        ArgumentNullException.ThrowIfNull(entities);
        Entities = entities;
        byType = entities.ToDictionary(e => e.EntityType);
    }

    public IReadOnlyList<EntityMapping> Entities { get; }

    public EntityMapping? Find(Type entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        for (var type = entityType; type is not null; type = type.BaseType)
        {
            if (byType.TryGetValue(type, out var mapping))
            {
                return mapping;
            }
        }

        return null;
    }

    public static ClusterMapping Build(Action<IClusterMappingBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new ClusterMappingBuilder();
        configure(builder);
        return builder.Build();
    }
}
