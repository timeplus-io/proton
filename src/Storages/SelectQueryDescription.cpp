#include <Storages/SelectQueryDescription.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>

/// proton: starts.
#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <TableFunctions/TableFunctionFactory.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

SelectQueryDescription::SelectQueryDescription(const SelectQueryDescription & other)
    : select_table_ids(other.select_table_ids)
    , inner_query(other.inner_query ? other.inner_query->clone() : nullptr)
{
}

SelectQueryDescription & SelectQueryDescription::SelectQueryDescription::operator=(const SelectQueryDescription & other)
{
    if (&other == this)
        return *this;

    select_table_ids = other.select_table_ids;
    if (other.inner_query)
        inner_query = other.inner_query->clone();
    else
        inner_query.reset();
    return *this;
}


namespace
{
ASTSelectWithUnionQuery * tryGetSubquery(const ASTPtr ast)
{
    if (!ast)
        return nullptr;

    auto * ast_select = ast->as<ASTSelectWithUnionQuery>();
    if (!ast_select)
    {
        /// support cte subquery. For example:
        /// WITH (SELECT 1) AS a SELECT * FROM a AS b; cte_name will be `a`.
        if (auto * subquery = ast->as<ASTSubquery>())
        {
            assert(subquery->children.size() == 1);
            ast_select = subquery->children[0]->as<ASTSelectWithUnionQuery>();
        }
    }
    return ast_select;
}

/// proton: starts. For streaming materialized view, we don't need to limit the number of selects anymore
void extractFromTableFunction(const ASTFunction & ast_func, std::vector<StorageID> & storage_ids, ContextPtr context);

void extractDependentTableFromSelectQuery(
    std::vector<StorageID> & storage_ids, ASTSelectWithUnionQuery & select_query, ContextPtr context, bool add_default_db = true)
{
    if (add_default_db)
    {
        AddDefaultDatabaseVisitor visitor(context, context->getCurrentDatabase());
        visitor.visit(select_query);
    }

    for (auto & query : select_query.list_of_selects->children)
    {
        for (const ASTTableExpression * table_expression : getTableExpressions(query->as<ASTSelectQuery &>()))
        {
            /// stream name
            if (auto storage_id_opt = tryGetStorageID(table_expression->database_and_table_name))
                storage_ids.emplace_back(*storage_id_opt);
            /// <window_func>(<stream_name>|<cte_subquery>, ...)
            else if (table_expression->table_function)
                extractFromTableFunction(table_expression->table_function->as<ASTFunction &>(), storage_ids, context);
            /// (subquery)
            else if (auto * subquery = tryGetSubquery(table_expression->subquery))
                extractDependentTableFromSelectQuery(storage_ids, *subquery, context, false);
            else
                throw Exception(
                    "Logical error while creating VIEW or MATERIALIZED VIEW. Could not retrieve stream name from select query",
                    DB::ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void extractFromTableFunction(const ASTFunction & ast_func, std::vector<StorageID> & storage_ids, ContextPtr context)
{
    assert(ast_func.arguments);
    const auto & table_arg = ast_func.arguments->children.at(0);
    if (auto storage_id_opt = tryGetStorageID(table_arg))
        storage_ids.emplace_back(*storage_id_opt); /// <stream_name>
    else if (auto * subquery = tryGetSubquery(table_arg))
        extractDependentTableFromSelectQuery(storage_ids, *subquery, context, false); /// <cte_subquery>
    else if (auto * nested_func = table_arg->as<ASTFunction>();
             nested_func && TableFunctionFactory::instance().isTableFunctionName(nested_func->name))
        extractFromTableFunction(*nested_func, storage_ids, context); /// <nested_table_function>
    else
        throw Exception(
            ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
            "'{}' is not supported while creating VIEW or MATERIALIZED VIEW",
            ast_func.getAliasOrColumnName());
}

void checkAllowedQueries(const ASTSelectWithUnionQuery & select_query)
{
    for (auto & inner_query : select_query.list_of_selects->children)
    {
        auto & query = inner_query->as<ASTSelectQuery &>();
        if (query.prewhere() || query.final() || query.sampleSize())
            throw Exception("MATERIALIZED VIEW cannot have PREWHERE, SAMPLE or FINAL.", DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

        for (const ASTTableExpression * table_expression : getTableExpressions(query))
        {
            if (auto * ast_select = tryGetSubquery(table_expression->subquery))
                checkAllowedQueries(*ast_select);
        }
    }
}

}

/// Rename to `getSelectQueryFromASTForView`
SelectQueryDescription SelectQueryDescription::getSelectQueryFromASTForView(const ASTPtr & select, ContextPtr context)
{
    SelectQueryDescription result;
    result.inner_query = select->clone();

    /// Propagate WITH elements to inner query
    if (context->getSettingsRef().enable_global_with_statement)
        ApplyWithAliasVisitor().visit(result.inner_query);
    ApplyWithSubqueryVisitor().visit(result.inner_query);

    auto & select_query = result.inner_query->as<ASTSelectWithUnionQuery &>();
    checkAllowedQueries(select_query);
    extractDependentTableFromSelectQuery(result.select_table_ids, select_query, context);
    return result;
}
/// proton: ends.

}
