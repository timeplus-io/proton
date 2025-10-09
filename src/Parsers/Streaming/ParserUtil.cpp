#include <Parsers/Streaming/ParserUtil.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
ASTPtr buildSelectFromTable(const StorageID & table_id, const Names & column_names)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    for (const auto & column_name : column_names)
    {
        auto identifier = std::make_shared<ASTIdentifier>(column_name);
        select_query->select()->children.push_back(identifier);
    }

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    tables->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);

    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(table_id);
    table_expr->children.push_back(table_expr->database_and_table_name);

    return select_query;
}
}
