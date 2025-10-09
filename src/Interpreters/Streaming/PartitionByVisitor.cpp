#include <Interpreters/Streaming/PartitionByVisitor.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/formatAST.h>

namespace DB
{
namespace
{
bool isStatefulFunction(const String & name, ContextPtr context)
{
    const auto & function = FunctionFactory::instance().tryGet(name, context);
    if (function && function->isStateful())
        return true;

    return false;
}
}

bool PartitionByMatcher::needChildVisit(ASTPtr &, ASTPtr &, Data &)
{
    /// Don't descent into table functions and subqueries and special case for ArrayJoin.
    return false;
}

void PartitionByMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto select = ast->as<ASTSelectQuery>())
    {
        visit(select->refSelect(), data);

        rewriteStatefulOverFunction(*select, data);

        rewriteByPartitionBy(*select, data);
    }
    else
    {
        if (auto * func = ast->as<ASTFunction>())
        {
            if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
            {
                if (func->is_window_function)
                    data.aggr_over_functions.push_back(func);
                else
                    data.aggregate_functions.push_back(func);
            }
            else if (isStatefulFunction(func->name, data.context))
            {
                if (func->is_window_function)
                    data.stateful_over_functions.push_back(func);
                else
                    data.stateful_functions.push_back(func);
            }
        }

        for (auto & child : ast->children)
            visit(child, data);
    }
}

void PartitionByMatcher::rewriteStatefulOverFunction(ASTSelectQuery & select, Data & data)
{
    if (data.stateful_over_functions.empty())
        return;

    String conflicting_func_name;
    if (!data.aggr_over_functions.empty())
        conflicting_func_name = fmt::format("aggregate over function '{}'", data.aggr_over_functions.front()->formatForErrorMessage());
    else if (!data.stateful_functions.empty())
        conflicting_func_name = fmt::format("stateful function '{}'", data.stateful_functions.front()->formatForErrorMessage());
    else if (!data.aggregate_functions.empty())
        conflicting_func_name = fmt::format("aggregate function '{}'", data.aggregate_functions.front()->formatForErrorMessage());

    if (!conflicting_func_name.empty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Conflicting functions detected: stateful over function '{}' and {} within the same layer are not supported. "
            "Please ensure that stateful over functions are used in isolation or split them into separate layers to avoid conflicts.",
            data.stateful_over_functions.front()->formatForErrorMessage(),
            conflicting_func_name);

    /// Try rewrite simple window (over partition by only), for example:
    /// Case-1: 'SELECT lag(x) OVER (PARTITION BY id), lag(y) OVER (PARTITION BY id) FROM t'
    ///     =>  'SELECT lag(x), lag(y) FROM t PARTITION BY id'
    ASTPtr partition_by = select.partitionBy();
    bool all_are_same_simple_window = true;
    for (const auto & window_func : data.stateful_over_functions)
    {
        if (window_func->window_definition)
        {
            const auto & definition = window_func->window_definition->as<const ASTWindowDefinition &>();
            if (definition.partition_by && !definition.order_by && definition.frame_is_default)
            {
                if (!partition_by)
                {
                    partition_by = definition.partition_by;
                    continue;
                }
                else if (partition_by->getTreeHash() == definition.partition_by->getTreeHash())
                    continue;
            }
        }

        all_are_same_simple_window = false;
        break;
    }

    if (select.partitionBy() && select.partitionBy()->getTreeHash() != partition_by->getTreeHash())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Conflicting PARTITION BY clauses detected: 'PARTITION BY {}' and 'OVER (PARTITION BY {})' within the same layer "
            "are not supported. Please ensure all PARTITION BY clauses in the same layer are identical or split them into separate "
            "layers.",
            select.partitionBy()->formatForErrorMessage(),
            partition_by->formatForErrorMessage());

    if (all_are_same_simple_window && partition_by)
    {
        for (auto & window_func : data.stateful_over_functions)
        {
            window_func->is_window_function = false;
            window_func->window_name = "";
            window_func->window_definition.reset();

            data.stateful_functions.push_back(window_func);
        }

        data.stateful_over_functions.clear();

        select.setExpression(ASTSelectQuery::Expression::PARTITION_BY, partition_by->clone());
    }

    if (!data.stateful_over_functions.empty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Stateful over function '{}' is not supported in non-simple window processing. Please ensure that only simple "
            "(partition-by-only) and same windows are used",
            data.stateful_over_functions.front()->formatForErrorMessage());
}

void PartitionByMatcher::rewriteByPartitionBy(ASTSelectQuery & select, Data & data)
{
    auto partition_by = select.partitionBy();
    if (!partition_by)
        return;

    /// If exist aggregates or group by, append `PARTITION BY` keys to `GROUP BY` expression
    if (select.groupBy())
    {
        auto & group_exprs = select.groupBy()->children;
        for (const auto & column_ast : partition_by->children)
        {
            auto key_name = column_ast->getColumnName();
            if (std::ranges::any_of(group_exprs, [&](const auto & ast) { return ast->getColumnName() == key_name; }))
                continue; /// Skip duplicated key

            group_exprs.emplace_back(column_ast->clone());
        }
    }
    else if (!data.aggregate_functions.empty())
    {
        select.setExpression(ASTSelectQuery::Expression::GROUP_BY, partition_by->clone());
    }
}
}
