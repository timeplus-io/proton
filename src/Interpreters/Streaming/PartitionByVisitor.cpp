#include "PartitionByVisitor.h"

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>

namespace DB
{

namespace
{
std::unordered_set<String> ignore_funcs = {"emit_version"};
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
            if (!data.win_define || node_func->is_window_function || ignore_funcs.contains(node_func->name))
                return;

            /// Convert functoin to window function.
            node_func->code_name = node_func->getColumnName();
            node_func->window_definition = data.win_define->clone();
            node_func->is_window_function = true;
        }
        else
            for (auto & child : ast->children)
                visit(child, data);
    }
}

}
