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
    if (AggregateFunctionFactory::instance().isAggregateFunctionName(name))
        return true;

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
        auto partition = select->partitionBy();
        if (!partition)
            return;

        auto win_def = std::make_shared<ASTWindowDefinition>();
        win_def->partition_by = partition;
        data.win_define = win_def;
        visit(select->refSelect(), data);
    }
    else
    {
        if (auto * node_func = ast->as<ASTFunction>())
        {
            if (!data.win_define || node_func->is_window_function || !isStatefulFunction(node_func->name, data.context))
                return;

            /// Convert function to window function.
            /// Always show original function
            node_func->makeCurrentCodeName();
            node_func->window_definition = data.win_define->clone();
            node_func->is_window_function = true;
        }
        else
            for (auto & child : ast->children)
                visit(child, data);
    }
}

}
