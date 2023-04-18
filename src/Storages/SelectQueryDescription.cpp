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
#include <Parsers/ASTWithElement.h>
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
struct ExtractDependentTableMatcher
{
    struct Data
    {
        std::vector<StorageID> & storage_ids;
        ContextPtr context;
    };

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (auto select = node->as<ASTSelectQuery>())
            return child.get() != select->with().get();

        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (const auto * table = ast->as<ASTTableExpression>())
            visit(*table, data);
        else if (const auto * func = ast->as<ASTFunction>())
            visit(*func, data);
    }

private:
    static void visit(const ASTTableExpression & table_expression, Data & data)
    {
        /// stream name
        if (auto storage_id_opt = tryGetStorageID(table_expression.database_and_table_name))
            data.storage_ids.emplace_back(*storage_id_opt);
    }

    static void visit(const ASTFunction & ast_func, Data & data)
    {
        if (TableFunctionFactory::instance().isTableFunctionName(ast_func.name))
        {
            assert(ast_func.arguments);
            const auto & table_arg = ast_func.arguments->children.at(0);
            if (auto storage_id_opt = tryGetStorageID(table_arg))
                data.storage_ids.emplace_back(*storage_id_opt);
        }
        else if (functionIsInOrGlobalInOperator(ast_func.name))
        {
            assert(ast_func.arguments);
            const auto & table_arg = ast_func.arguments->children.at(1);
            if (auto storage_id_opt = tryGetStorageID(table_arg))
                data.storage_ids.emplace_back(*storage_id_opt);
        }
        else if (functionIsDictGet(ast_func.name))
        {
            assert(ast_func.arguments);
            const auto & table_arg = ast_func.arguments->children.at(0);
            if (auto storage_id_opt = tryGetStorageID(table_arg))
                data.storage_ids.emplace_back(*storage_id_opt);
        }
    }
};

using ExtractDependentTableVisitor = ConstInDepthNodeVisitor<ExtractDependentTableMatcher, true>;

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

    AddDefaultDatabaseVisitor(context, context->getCurrentDatabase()).visit(select_query);

    ExtractDependentTableVisitor::Data data{result.select_table_ids, context};
    ExtractDependentTableVisitor(data).visit(select_query.ptr());
    return result;
}
/// proton: ends.

}
