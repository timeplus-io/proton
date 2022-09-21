#include <Storages/SelectQueryDescription.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>

/// proton: starts.
#include <Interpreters/Streaming/WindowCommon.h>
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
            {
                storage_ids.emplace_back(*storage_id_opt);
            }
            /// <window_func>(<stream_name>|<cte_subquery>, ...)
            else if (table_expression->table_function)
            {
                auto * ast_func = table_expression->table_function->as<ASTFunction>();
                assert(ast_func && ast_func->arguments);
                if (auto storage_id_opt = tryGetStorageID(ast_func->arguments->children.at(0)))
                    storage_ids.emplace_back(*storage_id_opt); /// <stream_name>
                else if (auto * subquery = tryGetSubquery(ast_func->arguments->children.at(0)))
                    extractDependentTableFromSelectQuery(storage_ids, *subquery, context, false); /// <cte_subquery>
                else
                    throw Exception(
                        ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
                        "'{}' is not supported for MATERIALIZED VIEW",
                        ast_func->getAliasOrColumnName());
            }
            /// (subquery)
            else if (auto * subquery = tryGetSubquery(table_expression->subquery))
            {
                extractDependentTableFromSelectQuery(storage_ids, *subquery, context, false);
            }
            else
                throw Exception(
                    "Logical error while creating MATERIALIZED VIEW. Could not retrieve stream name from select query",
                    DB::ErrorCodes::LOGICAL_ERROR);
        }
    }
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

SelectQueryDescription SelectQueryDescription::getSelectQueryFromASTForMatView(const ASTPtr & select, ContextPtr context)
{
    SelectQueryDescription result;
    result.inner_query = select->clone();
    auto & select_query = result.inner_query->as<ASTSelectWithUnionQuery &>();
    checkAllowedQueries(select_query);
    extractDependentTableFromSelectQuery(result.select_table_ids, select_query, context);
    return result;
}
/// proton: ends.

}
