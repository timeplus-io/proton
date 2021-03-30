#include <Interpreters/EliminateSubqueryVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{
void EliminateSubqueryVisitorData::visit(ASTSelectQuery & select_query, ASTPtr &)
{
    /// Ignore "join" case
    if (select_query.tables() == nullptr || select_query.tables()->children.size() != 1)
    {
        return;
    }

    for (auto & table : select_query.tables()->children)
    {
        if (auto * tableElement = table->as<ASTTablesInSelectQueryElement>())
        {
            if (auto * tableExpression = tableElement->table_expression->as<ASTTableExpression>())
            {
                visit(*tableExpression, select_query);
            }
        }
    }
}

void EliminateSubqueryVisitorData::visit(ASTTableExpression & table, ASTSelectQuery & parent_select)
{
    if (table.subquery == nullptr)
    {
        return;
    }

    if (table.subquery->children.size() != 1)
    {
        return;
    }

    auto * select_with_union_query = table.subquery->children.at(0)->as<ASTSelectWithUnionQuery>();
    if (!select_with_union_query || select_with_union_query->list_of_selects->children.size() != 1)
    {
        return;
    }

    auto & sub_query_node = select_with_union_query->list_of_selects->children.at(0);
    auto * sub_query = sub_query_node->as<ASTSelectQuery>();
    if (!sub_query)
    {
        return;
    }

    /// Handle sub query in table expression recursively
    visit(*sub_query, sub_query_node);

    if (sub_query->groupBy() || sub_query->having() || sub_query->orderBy() || sub_query->limitBy() || sub_query->limitByLength()
        || sub_query->limitByOffset() || sub_query->limitLength() || sub_query->limitOffset() || sub_query->distinct || sub_query->with())
        return;

    /// Try to eliminate subquery
    if (!mergeColumns(parent_select, *sub_query))
    {
        return;
    }

    if (sub_query->where() && parent_select.where())
    {
        auto where = makeASTFunction("and", sub_query->where(), parent_select.where());
        parent_select.setExpression(ASTSelectQuery::Expression::WHERE, where);
    }
    else if (sub_query->where())
    {
        parent_select.setExpression(ASTSelectQuery::Expression::WHERE, std::move(sub_query->refWhere()));
    }

    if (sub_query->prewhere() && parent_select.prewhere())
    {
        auto prewhere = makeASTFunction("and", sub_query->prewhere(), parent_select.prewhere());
        parent_select.setExpression(ASTSelectQuery::Expression::PREWHERE, prewhere);
    }
    else if (sub_query->prewhere())
    {
        parent_select.setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(sub_query->refPrewhere()));
    }
    parent_select.setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query->refTables()));
}

void EliminateSubqueryVisitorData::rewriteColumns(
    ASTPtr & ast, const std::unordered_map<String, ASTPtr> & subquery_selects, bool drop_alias /*= false*/)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        auto it = subquery_selects.find(identifier->name());
        if (it != subquery_selects.end())
        {
            String alias = ast->tryGetAlias();
            ast = it->second;
            if (drop_alias)
            {
                ast->setAlias("");
            }
            else if (!alias.empty())
            {
                ast->setAlias(alias);
            }
        }
    }
    else
    {
        for (auto & child : ast->children)
        {
            rewriteColumns(child, subquery_selects, true);
        }
    }
}

bool EliminateSubqueryVisitorData::mergeColumns(ASTSelectQuery & parent_query, ASTSelectQuery & child_query)
{
    /// E.g., select sum(b) from (select id as b from table)
    std::unordered_map<String, ASTPtr> subquery_selects;
    for (auto & column : child_query.select()->children)
    {
        if (column->as<ASTAsterisk>() || column->as<ASTQualifiedAsterisk>())
        {
            continue;
        }
        else if (column->as<ASTIdentifier>())
        {
            subquery_selects.emplace(column->getAliasOrColumnName(), column);
        }
        else
        {
            return false;
        }
    }

    /// Try to merge select columns
    for (auto & parent_select_item : parent_query.select()->children)
    {
        rewriteColumns(parent_select_item, subquery_selects);
    }
    return true;
}

}
