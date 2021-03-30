#include <Interpreters/AddTimeParamVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/TimeParam.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>


namespace DB
{
bool AddTimeVisitorMatcher::containTimeField(ASTPtr & node, Context & context)
{
    if (!node->as<ASTIdentifier>())
    {
        return false;
    }

    ASTIdentifier * table_identifier_node = node->as<ASTIdentifier>();
    auto storage_id(*table_identifier_node);
    auto table_id = context.resolveStorageID(storage_id);
    auto db = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    auto table = db->tryGetTable(table_id.table_name, context);

    if (!table)
    {
        return false;
    }

    auto metadata = table->getInMemoryMetadataPtr();
    const auto & col_desc = metadata->getColumns();
    return col_desc.has("_time") && col_desc.get("_time").type->getTypeId() == TypeIndex::DateTime64;
}

void AddTimeVisitorMatcher::visitSelectQuery(ASTPtr & ast, Context & context)
{
    if (!ast->as<ASTSelectQuery>())
    {
        return;
    }

    ASTSelectQuery * select = ast->as<ASTSelectQuery>();
    ASTPtr tables = select->tables();

    if (tables->children.empty())
    {
        return;
    }

    ASTPtr node = tables->children[0];
    if (!node->as<ASTTablesInSelectQueryElement>())
    {
        return;
    }

    ASTTablesInSelectQueryElement * first_table = node->as<ASTTablesInSelectQueryElement>();
    ASTTableExpression * table_expression = first_table->table_expression->as<ASTTableExpression>();

    if (table_expression->database_and_table_name)
    {
        insertTimeParamTime(select, table_expression->database_and_table_name, context);
    }
    else if (table_expression->subquery)
    {
        ASTPtr subquery = table_expression->subquery->children[0];
        if (subquery->as<ASTSelectWithUnionQuery>())
        {
            visitSelectWithUnionQuery(subquery, context);
        }
        else if (subquery->as<ASTSelectQuery>())
        {
            visitSelectQuery(subquery, context);
        }
    }
}

void AddTimeVisitorMatcher::insertTimeParamTime(ASTSelectQuery * select, ASTPtr & table_name, Context & context)
{
    ParserExpressionWithOptionalAlias elem_parser(false);
    if (!containTimeField(table_name, context))
    {
        return;
    }

    /// Merge time picker predicates into the where subtree of this select node
    /// BE Careful: where_statement may be null, when the sql doesn't contain where expression
    ASTPtr where_statement = select->where();
    ASTPtr new_node;
    if (!context.getTimeParam().getStart().empty())
    {
        new_node = parseQuery(
            elem_parser,
            context.getTimeParam().getStart(),
            context.getSettingsRef().max_query_size,
            context.getSettingsRef().max_parser_depth);
        new_node = makeASTFunction("greaterOrEquals", std::make_shared<ASTIdentifier>("_time"), new_node);
    }

    if (!context.getTimeParam().getEnd().empty())
    {
        ASTPtr less = parseQuery(
            elem_parser,
            context.getTimeParam().getEnd(),
            context.getSettingsRef().max_query_size,
            context.getSettingsRef().max_parser_depth);
        less = makeASTFunction("less", std::make_shared<ASTIdentifier>("_time"), less);
        new_node = new_node ? makeASTFunction("and", less, new_node) : less;
    }

    where_statement = where_statement ? makeASTFunction("and", new_node, where_statement) : new_node;
    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_statement));
}

void AddTimeVisitorMatcher::visitSelectWithUnionQuery(ASTPtr & ast, Context & context)
{
    if (!ast->as<ASTSelectWithUnionQuery>())
    {
        return;
    }

    const ASTSelectWithUnionQuery * un = ast->as<ASTSelectWithUnionQuery>();
    if (un->list_of_selects->children.empty())
    {
        return;
    }

    for (auto & child : un->list_of_selects->children)
    {
        if (child->as<ASTSelectQuery>())
        {
            visitSelectQuery(un->list_of_selects->children[0], context);
        }
    }
}

void AddTimeVisitorMatcher::visit(ASTPtr & ast, Context & context)
{
    if (ast->as<ASTSelectQuery>())
    {
        visitSelectQuery(ast, context);
    }
    else if (ast->as<ASTSelectWithUnionQuery>())
    {
        visitSelectWithUnionQuery(ast, context);
    }
}

}
