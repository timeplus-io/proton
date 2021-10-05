#include <Interpreters/UnnestSubqueryVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/formatAST.h>

namespace DB
{
void UnnestSubqueryVisitorData::visit(ASTSelectQuery & select_query, ASTPtr &)
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

void UnnestSubqueryVisitorData::visit(ASTTableExpression & table, ASTSelectQuery & parent_select)
{
    if (table.subquery == nullptr)
    {
        return;
    }

    if (table.subquery->children.size() != 1 || !table.subquery->tryGetAlias().empty())
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
    bool asterisk_in_subquery = false;
    std::unordered_map<String, ASTPtr> subquery_selects_map;
    ASTs subquery_selects;
    subquery_selects_map.reserve(sub_query->size());
    subquery_selects.reserve(sub_query->size());
    if (!mergeable(*sub_query, parent_select.select()->children, asterisk_in_subquery, subquery_selects_map, subquery_selects))
    {
        return;
    }

    /// Rewrite selects of parent query
    ASTPtr new_parent_group_by = std::make_shared<ASTExpressionList>();
    if (parent_select.groupBy())
    {
        setParentColumns(new_parent_group_by->children, false, parent_select.groupBy()->children, subquery_selects_map, subquery_selects);
    }

    ASTPtr new_parent_select = std::make_shared<ASTExpressionList>();
    setParentColumns(
        new_parent_select->children, asterisk_in_subquery, parent_select.select()->children, subquery_selects_map, subquery_selects);

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

    if (parent_select.groupBy())
    {
        parent_select.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(new_parent_group_by));
    }

    parent_select.setExpression(ASTSelectQuery::Expression::TABLES, std::move(sub_query->refTables()));

    parent_select.setExpression(ASTSelectQuery::Expression::SELECT, std::move(new_parent_select));
}

void UnnestSubqueryVisitorData::rewriteColumn(
    ASTPtr & ast, std::unordered_map<String, ASTPtr> & subquery_selects_map, bool drop_alias /*= false*/)
{
    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        auto it = subquery_selects_map.find(identifier->name());
        if (it != subquery_selects_map.end())
        {
            String alias = ast->tryGetAlias();
            ast = it->second->clone();
            if (drop_alias)
            {
                ast->setAlias("");
            }
            else if (!alias.empty())
            {
                ast->setAlias(alias);
            }
            subquery_selects_map.erase(it);
        }
    }
    else
    {
        String alias = ast->tryGetAlias();

        if (alias.empty() && ast->as<ASTFunction>())
        {
            alias = serializeAST(*ast);
        }

        for (auto & child : ast->children)
        {
            rewriteColumn(child, subquery_selects_map, true);
        }

        if (alias != serializeAST(*ast) && ast->as<ASTFunction>())
        {
            ast->setAlias(alias);
        }
    }
}

bool UnnestSubqueryVisitorData::mergeable(
    const ASTSelectQuery & child_query,
    const ASTs & parent_selects,
    bool & asterisk_in_subquery,
    std::unordered_map<String, ASTPtr> & subquery_selects_map,
    ASTs & subquery_selects)
{
    /// E.g., select sum(b) from (select id as b from table)
    for (auto & column : child_query.select()->children)
    {
        if (column->as<ASTAsterisk>() || column->as<ASTQualifiedAsterisk>())
        {
            if (!asterisk_in_subquery)
            {
                asterisk_in_subquery = true;
                subquery_selects.push_back(column);
            }
            continue;
        }
        else if (auto * identifier = column->as<ASTIdentifier>())
        {
            auto it = subquery_selects_map.find(identifier->getAliasOrColumnName());
            if (it == subquery_selects_map.end())
            {
                subquery_selects.push_back(column);
                subquery_selects_map.emplace(identifier->getAliasOrColumnName(), column);
            }
        }
        else
        {
            return false;
        }
    }

    if (!asterisk_in_subquery)
    {
        for (const auto & parent_select_item : parent_selects)
        {
            if (!isValid(parent_select_item, subquery_selects_map))
            {
                return false;
            }
        }
    }

    return true;
}

bool UnnestSubqueryVisitorData::isValid(const ASTPtr & ast, const std::unordered_map<String, ASTPtr> & subquery_selects_map) const
{
    if (ast->as<ASTAsterisk>() || ast->as<ASTQualifiedAsterisk>())
    {
        return true;
    }

    if (auto * identifier = ast->as<ASTIdentifier>())
    {
        const String & column_name = identifier->getColumnName();
        if (subquery_selects_map.find(column_name) == subquery_selects_map.end())
        {
            return false;
        }
    }

    for (auto & child : ast->children)
    {
        if (!isValid(child, subquery_selects_map))
        {
            return false;
        }
    }

    return true;
}


void UnnestSubqueryVisitorData::setParentColumns(
    ASTs & new_parent_selects,
    bool asterisk_in_subquery,
    const ASTs & parent_selects,
    std::unordered_map<String, ASTPtr> & subquery_selects_map,
    const ASTs & subquery_selects)
{
    /// Subquery selects having alias
    ASTs subquery_selects_with_alias;
    if (asterisk_in_subquery)
    {
        subquery_selects_with_alias.reserve(subquery_selects.size());
        for (const auto & subquery_select_item : subquery_selects)
        {
            if (auto * identifier = subquery_select_item->as<ASTIdentifier>())
            {
                if (!identifier->tryGetAlias().empty())
                {
                    subquery_selects_with_alias.push_back(subquery_select_item);
                }
            }
            else
            {
                /// `subquery_select_item` is ASTAsterisk or ASTQualifiedAsterisk
                subquery_selects_with_alias.push_back(subquery_select_item);
            }
        }
    }

    for (const auto & parent_select_item : parent_selects)
    {
        if (parent_select_item->as<ASTAsterisk>())
        {
            if (!asterisk_in_subquery)
            {
                /// No asterisk in subquery selects
                new_parent_selects.insert(new_parent_selects.end(), subquery_selects.begin(), subquery_selects.end());
            }
            else
            {
                new_parent_selects.insert(new_parent_selects.end(), subquery_selects_with_alias.begin(), subquery_selects_with_alias.end());
            }
        }
        else
        {
            new_parent_selects.push_back(parent_select_item);
        }
    }

    /// Try to rewrite columns of parent select
    for (auto & parent_select_item : new_parent_selects)
    {
        rewriteColumn(parent_select_item, subquery_selects_map);
    }
}

}
