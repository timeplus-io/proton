#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>

#include <Common/ProtonCommon.h>

/// proton: starts.
namespace
{
    bool hasSessionWindowColumn(const DB::ASTPtr & ast)
    {
        if (const auto * function = ast->as<DB::ASTFunction>())
        {
            if (const auto * args = function->arguments->as<DB::ASTExpressionList>())
            {
                if (args->children.empty())
                    return false;

                for (const auto & arg : args->children)
                    if (hasSessionWindowColumn(arg))
                        return true;
                return false;
            }
            else
                return false;
        }
        else if (const auto * identifier = ast->as<DB::ASTIdentifier>())
        {
            return std::find(DB::ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.begin(), DB::ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end(), identifier->name())
                != DB::ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES.end();
        }
        else
            return false;
    }
}
/// proton: ends.

namespace DB
{

void ExpressionInfoMatcher::visit(const ASTPtr & ast, Data & data)
{
    if (const auto * function = ast->as<ASTFunction>())
        visit(*function, ast, data);
    else if (const auto * identifier = ast->as<ASTIdentifier>())
        visit(*identifier, ast, data);
}

void ExpressionInfoMatcher::visit(const ASTFunction & ast_function, const ASTPtr & ast, Data & data)
{
    if (ast_function.name == "array_join")
    {
        data.is_array_join = true;
    }
    // "is_aggregate_function" is used to determine whether we can move a filter
    // (1) from HAVING to WHERE or (2) from WHERE of a parent query to HAVING of
    // a subquery.
    // For aggregate functions we can't do (1) but can do (2).
    // For window functions both don't make sense -- they are not allowed in
    // WHERE or HAVING.
    else if (!ast_function.is_window_function
        && AggregateFunctionFactory::instance().isAggregateFunctionName(
            ast_function.name))
    {
        data.is_aggregate_function = true;
    }
    else if (ast_function.is_window_function)
    {
        data.is_window_function = true;
    }
    else
    {
        const auto & function = FunctionFactory::instance().tryGet(ast_function.name, data.getContext());

        /// Skip lambda, tuple and other special functions
        if (function)
        {
            if (function->isStateful())
                data.is_stateful_function = true;

            if (!function->isDeterministicInScopeOfQuery())
                data.is_deterministic_function = false;
        }

        /// proton: starts.
        /// For session window, window_start/window_end are not generated before aggregation step.
        /// The predication statement which contains window_start/window_end could not be moved to where clause.
        if (hasSessionWindowColumn(ast))
            data.is_aggregate_function = true;
        /// proton: ends.
    }
}

void ExpressionInfoMatcher::visit(const ASTIdentifier & identifier, const ASTPtr &, Data & data)
{
    if (!identifier.compound())
    {
        for (size_t index = 0; index < data.tables.size(); ++index)
        {
            const auto & table = data.tables[index];

            // TODO: make sure no collision ever happens
            if (table.hasColumn(identifier.name()))
            {
                data.unique_reference_tables_pos.emplace(index);
                break;
            }
        }
    }
    else
    {
        if (auto best_table_pos = IdentifierSemantic::chooseTable(identifier, data.tables))
            data.unique_reference_tables_pos.emplace(*best_table_pos);
    }
}

bool ExpressionInfoMatcher::needChildVisit(const ASTPtr & node, const ASTPtr &)
{
    return !node->as<ASTSubquery>();
}

bool hasNonRewritableFunction(const ASTPtr & node, ContextPtr context)
{
    for (const auto & select_expression : node->children)
    {
        TablesWithColumns tables;
        ExpressionInfoVisitor::Data expression_info{WithContext{context}, tables};
        ExpressionInfoVisitor(expression_info).visit(select_expression);

        if (expression_info.is_stateful_function
            || expression_info.is_window_function)
        {
            // If an outer query has a WHERE on window function, we can't move
            // it into the subquery, because window functions are not allowed in
            // WHERE and HAVING. Example:
            // select * from (
            //     select number,
            //          count(*) over (partition by intDiv(number, 3)) c
            //     from numbers(3)
            // ) where c > 1;
            return true;
        }
    }

    return false;
}

}

