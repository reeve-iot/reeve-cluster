using System.Linq.Expressions;

namespace Reeve.Cluster;

/// <summary>
/// The contract mapping entity types to aggregate types, keys and counted columns. The EF Core package implements and
/// consumes this mapping. Added or deleted mapped entities always produce entries; modifications produce entries when
/// counted columns change, or when the aggregate key changes (both old and new keys are emitted). A mapped base type
/// also covers its derived types, and several entity types may share an aggregate type. Keys are computed from CLR
/// members, so shadow keys require <c>NoKey()</c>; key functions should use key columns, not navigations.
/// </summary>
/// <remarks>
/// Owned and JSON-owned changes count for their mapped ownership root regardless of counted columns. Many-to-many link
/// changes count for each mapped side. Database cascades, and <c>SetNull</c> paths whose FK column is counted (or all
/// columns are counted), produce keyless entries for affected mapped aggregate types. Complex properties can be counted
/// by name alongside scalar properties. Capture sees changes made by <c>SaveChanges</c>, not bulk or raw SQL writes;
/// those require <see cref="IChangeLog.AppendAsync"/>. <c>ExecuteDelete</c> on a mapped type, and <c>ExecuteUpdate</c>
/// that sets a counted, key or owned column or any column of an all-columns type, throw unless the query carries
/// <c>AppendedToChangeLog()</c>; an update of only non-counted columns does not. The key function must read only counted or key
/// columns, or a bulk update of another column it reads is not seen. Register capture after interceptors that change entities in
/// <c>SavingChanges</c>. A save with cascade-delete or orphan-delete timing set to <c>OnSaveChanges</c> throws, because
/// EF would delete dependents after capture reads the tracker. A context saving without capture omits its entries from the
/// change log; start refuses one registered through <c>DbContextOptions&lt;T&gt;</c> that has mapped types.
/// </remarks>
public interface IClusterMappingBuilder
{
    /// <summary>Maps <typeparamref name="TEntity"/> to <paramref name="aggregateType"/>.</summary>
    /// <exception cref="InvalidOperationException">The entity type is already mapped.</exception>
    IEntityMappingBuilder<TEntity> Entity<TEntity>(string aggregateType)
        where TEntity : class;
}

/// <summary>
/// The mapping of one entity type. Both choices below are explicit: the mapping is column-scoped, so the application
/// names the columns that count or says that all do, and names the key or says there is none. A mapping missing
/// either is refused when the mapping is built.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public interface IEntityMappingBuilder<TEntity>
    where TEntity : class
{
    /// <summary>How the aggregate key of a changed entity is computed; the key is a hint that narrows a reconciler's re-read.</summary>
    IEntityMappingBuilder<TEntity> Key(Func<TEntity, string> key);

    /// <summary>The entries of this type carry no key: its reconciler always re-reads the whole type.</summary>
    IEntityMappingBuilder<TEntity> NoKey();

    /// <summary>Only a change to one of these scalar or complex properties produces an entry; a change to other columns produces none, unless it changes the aggregate key.</summary>
    IEntityMappingBuilder<TEntity> Columns(params Expression<Func<TEntity, object?>>[] properties);

    /// <summary>The same by name, for a shadow property that has no CLR member.</summary>
    IEntityMappingBuilder<TEntity> Columns(params string[] propertyNames);

    /// <summary>Every change to the entity produces an entry.</summary>
    IEntityMappingBuilder<TEntity> AllColumns();
}

/// <summary>The built mapping the capture reads.</summary>
public interface IClusterMapping
{
    /// <summary>Every mapped entity type.</summary>
    IReadOnlyList<EntityMapping> Entities { get; }

    /// <summary>The mapping of <paramref name="entityType"/>, or <see langword="null"/> when it is not configuration.</summary>
    EntityMapping? Find(Type entityType);
}

/// <summary>The mapping of one entity type.</summary>
/// <param name="EntityType">The CLR entity type.</param>
/// <param name="AggregateType">The aggregate type its changes belong to.</param>
/// <param name="Key">Computes the aggregate key of an entity of <paramref name="EntityType"/>; <see langword="null"/> when the type has no key.</param>
/// <param name="CountedProperties">The property names whose change counts; <see langword="null"/> when every change counts.</param>
public sealed record EntityMapping(
    Type EntityType,
    string AggregateType,
    Func<object, string>? Key,
    IReadOnlySet<string>? CountedProperties);
