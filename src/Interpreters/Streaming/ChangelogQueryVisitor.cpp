#include <Interpreters/Streaming/ChangelogQueryVisitor.h>
/// #include <Interpreters/RequiredSourceColumnsVisitor.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/ProtonCommon.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ALIAS_REQUIRED;
}

namespace
{
ASTPtr rewriteAsSubquery(ASTTableExpression & table_expr)
{
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
    /// SELECT * FROM default.vk1 INNER JOIN default.vk2 => We can't rewrite as
    /// SELECT * FROM (SELECT * FROM default.vk1) AS default.vk1 INNER JOIN default.vk2
    /// SELECT * FROM vk1 AS vvk1 INNER JOIN default.vk2 => We actually can't rewrite as
    /// SELECT * FROM (SELECT * FROM vk1) AS vvk1 INNER JOIN default.vk2 => Since after rewrite, vk1 is not visible any more
    /// FIXME, let's assume by all default database
    /// let's assume after alias, user will always use alias
    String alias;

    if (new_table_expr->database_and_table_name != nullptr)
    {
        auto & table_id = new_table_expr->database_and_table_name->as<ASTTableIdentifier &>();
        alias = table_id.alias;
        table_id.alias.clear();

        if (alias.empty())
            alias = table_id.shortName();

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

    assert(!alias.empty());

    subquery->setAlias(alias);

    return subquery;
}
}

namespace Streaming
{
void ChangelogQueryVisitorMatcher::visit(ASTSelectQuery & select_query, ASTPtr &)
{
    /// For join case, we will fix later
    const auto & tables = select_query.tables();
    if (tables == nullptr || tables->children.empty())
        return;

    if (join_strictness)
    {
        assert(right_input_data_stream_semantic);

        assert(tables->children.size() == 2);
        /// For join, replace the left / right stream with subquery if necessary

        auto rewrite = [&](ASTPtr & query_element, DataStreamSemantic data_stream_semantic) {
            auto & table_element = query_element->as<ASTTablesInSelectQueryElement &>();
            auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

            /// If table expression is a subquery, it shall already rewritten to include
            /// the _tp_delta column
            if (!table_expr.subquery)
            {
                bool need_rewrite = isChangelogDataStream(data_stream_semantic) && query_info.changelog_tracking_changes;
                need_rewrite = isVersionedKeyedDataStream(data_stream_semantic) && query_info.versioned_kv_tracking_changes;

                if (need_rewrite)
                    rewriteAsSubquery(table_expr);
            }
        };

        std::array<DataStreamSemantic, 2> semantics {left_input_data_stream_semantic, *right_input_data_stream_semantic};
        for (size_t i = 0; auto & query_element : tables->children)
            rewrite(query_element, semantics[i++]);
    }
    else
    {
        /// Single stream
        assert(tables->children.size() == 1);

        bool need_rewrite = isChangelogDataStream(left_input_data_stream_semantic) && query_info.changelog_tracking_changes;
        need_rewrite |= isVersionedKeyedDataStream(left_input_data_stream_semantic) && query_info.versioned_kv_tracking_changes;

        if (!need_rewrite || current_select_has_aggregates)
            /// Do nothing, since `_tp_delta` will be added to each aggregation function
            return;

        addDeltaColumn(select_query);
    }
}

void ChangelogQueryVisitorMatcher::rewriteAsSubQuery(ASTTableExpression & table_expr)
{
    /// SELECT vk.k, vk2.j FROM vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// SELECT vk.k, vk2.j FROM (SELECT * FROM vk) AS vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// Having alias
    /// SELECT vk.k, vk2.j FROM changelog(vk) AS vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// SELECT vk.k, vk2.j FROM (SELECT * FROM changelog(vk)) AS vk JOIN vk2 ON vk.k = vk2.k2; =>

    /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
    table_expr.subquery = rewriteAsSubquery(table_expr);
    table_expr.children.clear();
    table_expr.database_and_table_name = nullptr;
    table_expr.table_function = nullptr;
    table_expr.children.push_back(table_expr.subquery);
    hard_rewritten = true;
}

void ChangelogQueryVisitorMatcher::addDeltaColumn(ASTSelectQuery & select_query)
{
    /// For single stream, append `_tp_delta` to the select projection

    /// Parent select has join or aggregates, and current select is just a flat select
    /// add _tp_delta to select list if _tp_delta is not present.
    /// SELECT i, k FROM vk => SELECT i, k, _tp_delta FROM vk
    const auto select_expression_list = select_query.select();

    bool found_delta_col = false;
    for (const auto & selected_col : select_expression_list->children)
    {
        if (auto * id = selected_col->as<ASTIdentifier>(); id)
        {
            if (id->shortName() == ProtonConsts::RESERVED_DELTA_FLAG || id->name() == ProtonConsts::RESERVED_DELTA_FLAG)
            {
                found_delta_col = true;
                break;
            }
        }
        else if (selected_col->as<ASTAsterisk>())
        {
            found_delta_col = true;
            break;
        }
    }

    if (!found_delta_col)
        select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));

    if (add_new_required_result_columns)
        new_required_result_column_names.push_back(ProtonConsts::RESERVED_DELTA_FLAG);
}

}
}
