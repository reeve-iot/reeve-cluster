using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed class BulkOperationGuard : IQueryExpressionInterceptor
{
    public static BulkOperationGuard Instance { get; } = new();

    private BulkOperationGuard()
    {
    }

    public Expression QueryCompilationStarting(Expression queryExpression, QueryExpressionEventData eventData)
    {
        var bulk = queryExpression as MethodCallExpression;
        if (bulk is null || !IsBulkOperation(bulk.Method) || bulk.Arguments.Count == 0)
        {
            return queryExpression;
        }

        var context = eventData.Context;
        var mapping = context?.GetService<IDbContextOptions>()
            .FindExtension<ClusterCaptureOptionsExtension>()?.Mapping;
        if (context is null || mapping is null || IsMarked(bulk.Arguments[0]))
        {
            return queryExpression;
        }

        var rootType = bulk.Method.GetGenericArguments()[0];
        if (IsExecuteDelete(bulk.Method))
        {
            return MappedTypeOf(mapping, rootType) is { } deleted ? throw Refusal(deleted) : queryExpression;
        }

        var selectors = TryGetSelectors(bulk);
        if (selectors is null)
        {
            return MappedTypeOf(mapping, rootType) is { } unreadable ? throw Refusal(unreadable) : queryExpression;
        }

        foreach (var selector in selectors)
        {
            if (!TryGetTarget(context.Model, selector.Body, out var entityType, out var name))
            {
                if (MappedTypeOf(mapping, rootType) is { } unresolved)
                {
                    throw Refusal(unresolved);
                }

                continue;
            }

            if (CapturedWriteType(mapping, entityType, name) is { } captured)
            {
                throw Refusal(captured);
            }
        }

        return queryExpression;
    }

    private static bool IsBulkOperation(MethodInfo method) =>
        method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
        && method.IsGenericMethod
        && (IsExecuteDelete(method) || IsExecuteUpdate(method));

    private static bool IsExecuteDelete(MethodInfo method) => method.Name == nameof(EntityFrameworkQueryableExtensions.ExecuteDelete);

    private static bool IsExecuteUpdate(MethodInfo method) => method.Name == nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate);

    private static Type? MappedTypeOf(IClusterMapping mapping, Type type) =>
        mapping.Find(type)?.EntityType ?? mapping.Entities.FirstOrDefault(e => type.IsAssignableFrom(e.EntityType))?.EntityType;

    private static Type? CapturedWriteType(IClusterMapping mapping, IEntityType entityType, string name)
    {
        if (entityType.IsOwned() || entityType.FindNavigation(name)?.TargetEntityType.IsOwned() == true)
        {
            var root = entityType;
            while (root.IsOwned())
            {
                root = root.FindOwnership()!.PrincipalEntityType;
            }

            return MappedTypeOf(mapping, root.ClrType);
        }

        var maps = mapping.Find(entityType.ClrType) is { } own
            ? new[] { own }
            : mapping.Entities.Where(e => entityType.ClrType.IsAssignableFrom(e.EntityType)).ToArray();
        var property = entityType.FindProperty(name);
        var isKey = property is not null && (property.IsPrimaryKey() || property.IsForeignKey());
        return maps.FirstOrDefault(map => isKey || EntryCapture.IsCounted(map, name))?.EntityType;
    }

    private static List<LambdaExpression>? TryGetSelectors(MethodCallExpression bulk)
    {
        if (bulk.Arguments.Count < 2)
        {
            return null;
        }

        var setters = Unwrap(bulk.Arguments[1]) as NewArrayExpression;
        if (setters is null || setters.Expressions.Count == 0)
        {
            return null;
        }

        var selectors = new List<LambdaExpression>();
        foreach (var setter in setters.Expressions)
        {
            var tuple = Unwrap(setter) as NewExpression;
            if (tuple is null || tuple.Arguments.Count < 2 || GetLambda(tuple.Arguments[0]) is not { } selector)
            {
                return null;
            }

            selectors.Add(selector);
        }

        return selectors;
    }

    private static LambdaExpression? GetLambda(Expression expression)
    {
        expression = Unwrap(expression);
        if (expression is LambdaExpression lambda)
        {
            return lambda;
        }

        var constant = expression as ConstantExpression;
        return constant?.Value as LambdaExpression;
    }

    private static bool TryGetTarget(IModel model, Expression selectorBody, out IEntityType entityType, out string name)
    {
        var expression = Unwrap(selectorBody);
        while (true)
        {
            Type instanceType;
            string member;
            Expression instance;
            if (expression is MemberExpression { Expression: { } owner } memberExpression)
            {
                instance = owner;
                instanceType = owner.Type;
                member = memberExpression.Member.Name;
            }
            else if (expression is MethodCallExpression { Method: { Name: nameof(EF.Property), DeclaringType: { } declaring }, Arguments: [var target, ConstantExpression { Value: string propertyName }] }
                     && declaring == typeof(EF))
            {
                instance = target;
                instanceType = target.Type;
                member = propertyName;
            }
            else
            {
                break;
            }

            if (model.FindEntityType(instanceType) is { } found)
            {
                entityType = found;
                name = member;
                return true;
            }

            expression = Unwrap(instance);
        }

        entityType = null!;
        name = null!;
        return false;
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression unary && unary.NodeType is ExpressionType.Quote or ExpressionType.Convert or ExpressionType.ConvertChecked)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsMarked(Expression source)
    {
        var call = source as MethodCallExpression;
        while (call is not null)
        {
            if (call.Method.DeclaringType == typeof(EntityFrameworkQueryableExtensions)
                && call.Method.Name == nameof(EntityFrameworkQueryableExtensions.TagWith)
                && call.Arguments.Count > 1)
            {
                var constant = call.Arguments[1] as ConstantExpression;
                if (Equals(constant?.Value, ReeveClusterQueryableExtensions.AppendedToChangeLogTag))
                {
                    return true;
                }
            }

            if (call.Arguments.Count == 0)
            {
                return false;
            }

            call = call.Arguments[0] as MethodCallExpression;
        }

        return false;
    }

    private static InvalidOperationException Refusal(Type type)
    {
        var name = type.FullName ?? type.Name;
        return new InvalidOperationException($"Bulk operation on mapped entity type {name} is not captured: the change log does not see this ExecuteUpdate/ExecuteDelete, which writes a counted or key column or deletes. Append the changed aggregates with IChangeLog.AppendAsync in the same transaction and mark the query with .AppendedToChangeLog().");
    }
}
