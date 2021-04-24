#include "QueryProfileVisitor.h"

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

bool QueryProfileMatcher::needChildVisit(const ASTPtr & node, const ASTPtr & child)
{
    (void)node;
    (void)child;
    /// Every single child
    return true;
}

void QueryProfileMatcher::visit(ASTPtr & ast, Data & data)
{
    if (data.done())
        return;

    if (auto * t = ast->as<ASTSelectWithUnionQuery>())
    {
        if (t->list_of_selects->children.size() > 1)
            data.has_union = true;

        return;
    }

    if (auto * t = ast->as<ASTSelectQuery>())
    {
        if (t->groupBy())
            data.has_aggr = true;

        return;
    }

    if (ast->as<ASTSubquery>())
    {
        data.has_subquery = true;
        return;
    }

    if (auto * t = ast->as<ASTTablesInSelectQueryElement>())
    {
        for (const auto & child : t->children)
            if (child->as<ASTTableJoin>())
                data.has_table_join = true;
        return;
    }
}

}
