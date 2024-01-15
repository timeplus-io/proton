#include <Interpreters/Streaming/RewriteAsSubquery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ALIAS_REQUIRED;
}

namespace Streaming
{
namespace
{
bool rewriteAsChangelogQuery(ASTSelectWithUnionQuery & query)
{
    if (query.list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only expect one select query to rewrite as changelog query");

    auto & select_query = query.list_of_selects->children[0]->as<ASTSelectQuery &>();

    /// Emit changelog
    auto emit_query = select_query.emit();
    if (!emit_query)
        emit_query = std::make_shared<ASTEmitQuery>();

    if (emit_query->as<ASTEmitQuery &>().stream_mode == ASTEmitQuery::StreamMode::CHANGELOG)
        return false;

    emit_query->as<ASTEmitQuery &>().stream_mode = ASTEmitQuery::StreamMode::CHANGELOG;
    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(emit_query));
    return true;
}
}

ASTPtr rewriteAsSubquery(ASTTableExpression & table_expr)
{
    if (table_expr.subquery)
        return nullptr;

    auto subquery = std::make_shared<ASTSubquery>();
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    subquery->children.emplace_back(select_with_union_query);

    /// List of selects
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_with_union_query->list_of_selects->children.push_back(select_query);

    /// Select columns / expressions
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    auto select_expression_list = select_query->select();

    select_expression_list->children.emplace_back(std::make_shared<ASTAsterisk>());

    /// Table expression
    auto tables_in_select = std::make_shared<ASTTablesInSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select);

    auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select->children.push_back(tables_in_select_element);

    auto new_table_expr = std::make_shared<ASTTableExpression>();
    tables_in_select_element->children.emplace_back(new_table_expr);
    tables_in_select_element->table_expression = new_table_expr;

    new_table_expr->database_and_table_name = table_expr.database_and_table_name;
    new_table_expr->table_function = table_expr.table_function;

    /// If there is alias or long version storage name, things will become complicated
    /// SELECT * FROM default.vk1 INNER JOIN default.vk2
    /// SELECT * FROM (SELECT * FROM default.vk1)(CTE: vk1) INNER JOIN default.vk2
    /// SELECT * FROM vk1 AS vvk1 INNER JOIN default.vk2
    /// SELECT * FROM (SELECT * FROM vk1)(CTE: vk1) AS vvk1 INNER JOIN default.vk2
    /// FIXME, let's assume by all default database
    String table_name;
    String alias;

    if (new_table_expr->database_and_table_name != nullptr)
    {
        auto & table_id = new_table_expr->database_and_table_name->as<ASTTableIdentifier &>();
        table_name = table_id.shortName();
        alias = table_id.alias;
        table_id.alias.clear();

        new_table_expr->children.emplace_back(new_table_expr->database_and_table_name);
    }
    else if (new_table_expr->table_function != nullptr)
    {
        auto & func = new_table_expr->table_function->as<ASTFunction &>();
        if (func.alias.empty())
            throw Exception(ErrorCodes::ALIAS_REQUIRED, "Table function requires an alias in this query in this scenario");

        alias = func.alias;
        func.alias.clear();

        new_table_expr->children.emplace_back(new_table_expr->table_function);
    }
    else
        return nullptr; /// No rewrite for subquery

    subquery->cte_name = std::move(table_name);
    subquery->alias = std::move(alias);

    /// Rewrite table expr
    table_expr.subquery = subquery;
    table_expr.children.clear();
    table_expr.database_and_table_name = nullptr;
    table_expr.table_function = nullptr;
    table_expr.children.push_back(table_expr.subquery);
    return subquery;
}

bool rewriteAsChangelogSubquery(ASTTableExpression & table_expression, bool only_rewrite_subquery)
{
    auto subquery = table_expression.subquery;
    if (!subquery)
    {
        if (only_rewrite_subquery)
            return false;

        subquery = rewriteAsSubquery(table_expression);
        assert(subquery);
    }

    auto & query = subquery->as<ASTSubquery &>().children[0]->as<ASTSelectWithUnionQuery &>();
    return rewriteAsChangelogQuery(query);
}

bool rewriteSubquery(ASTSelectWithUnionQuery & query, const SelectQueryInfo & query_info)
{
    if (query_info.left_input_tracking_changes)
        return rewriteAsChangelogQuery(query);

    return false;
}
}
}
