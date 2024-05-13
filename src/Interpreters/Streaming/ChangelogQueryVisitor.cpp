#include <Interpreters/Streaming/ChangelogQueryVisitor.h>
/// #include <Interpreters/RequiredSourceColumnsVisitor.h>

#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_IDENTIFIER;
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
void ChangelogQueryVisitorMatcher::visit(ASTSelectQuery & select_query, ASTPtr &)
{
    /// For join case, we will fix later
    const auto & tables = select_query.tables();
    if (tables == nullptr || tables->children.empty())
        return;

    assert(tables->children.size() == tables_with_columns.size());
    /// For join, replace the left / right stream with subquery if necessary
    if (tables_with_columns.size() == 2)
    {
        /// Rewrite left input as a changelog subquery if required tracking changes
        if (query_info.left_input_tracking_changes)
            input_rewritten |= rewriteAsChangelogSubquery(
                tables->children[0]->as<ASTTablesInSelectQueryElement &>().table_expression->as<ASTTableExpression &>(),
                /*only_rewrite_subqeury*/ false);

        /// Rewrite right input as a changelog subquery if required tracking changes
        if (query_info.right_input_tracking_changes)
            input_rewritten |= rewriteAsChangelogSubquery(
                tables->children[1]->as<ASTTablesInSelectQueryElement &>().table_expression->as<ASTTableExpression &>(),
                /*only_rewrite_subqeury*/ false);

        /// Only add _tp_delta for subquery, skip top level select query, for exmaple:
        /// select s, ss from vk_1 inner join vk_2 on i=ii
        /// => select s, ss from (select s, ss, _tp_delta from vk_1) as vk_1 inner join (select s, ss, _tp_delta from vk_2) as vk_2 on i=ii
        ///
        /// select s, ss, sss from vk_1 inner join vk_2 on i=ii inner join vk_3 on i=iii
        /// => select s, ss, sss from (select s, ss from vk_1 inner join vk_2 on i=ii) as `--.s` inner join vk_3 on i=iii
        /// => select s, ss, sss from (select s, ss, _tp_delta from (select s, ss, _tp_delta from vk_1) as vk_1 inner join (select s, ss, _tp_delta from vk_2) as vk_2 on i=ii) as `--.s` inner join (select iii, sss, _tp_delta from vk_3) as vk_3 on i=iii
        if (data_stream_semantic_pair.isChangelogOutput())
            addDeltaColumn(select_query, /*asterisk_include_delta*/ true);
    }
    else
    {
        /// Single stream
        assert(tables->children.size() == 1);

        /// Rewrite `subquery` as a changelog query if required tracking changes
        /// Only rewrite the subquery emit changelog, since it shall already be converted to changelog in IStorage::read() if storage exists
        if (query_info.left_input_tracking_changes)
            input_rewritten |= rewriteAsChangelogSubquery(
                tables->children[0]->as<ASTTablesInSelectQueryElement &>().table_expression->as<ASTTableExpression &>(),
                /*only_rewrite_subqeury*/ true);

        if (data_stream_semantic_pair.isChangelogOutput())
        {
            /// For changelog/changelog_kv, the `*` include `_tp_delta`
            /// For versioned_kv, the `*` didn't include `_tp_delta`
            bool asterisk_include_delta = isChangelogDataStream(tables_with_columns.front().output_data_stream_semantic);
            addDeltaColumn(select_query, asterisk_include_delta);
        }
    }
}

void ChangelogQueryVisitorMatcher::addDeltaColumn(ASTSelectQuery & select_query, bool asterisk_include_delta)
{
    /// Append `_tp_delta` to the select projection
    /// Parent select has join or aggregates, and current select is just a flat select
    /// add _tp_delta to select list if _tp_delta is not present.
    /// SELECT i, k FROM vk => SELECT i, k, _tp_delta FROM vk
    const auto select_expression_list = select_query.select();

    bool found_delta_col = false;
    for (const auto & selected_col : select_expression_list->children)
    {
        if (auto * id = selected_col->as<ASTIdentifier>(); id)
        {
            if (id->name() == ProtonConsts::RESERVED_DELTA_FLAG)
            {
                if (!id->tryGetAlias().empty())
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED, "The identifier '{}' doesn't support an alias", id->formatForErrorMessage());

                /// FIXME: So far, we only expect only one `_tp_delta` during internal processing (such as join, .etc)
                if (found_delta_col && is_subquery)
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Multiple '_tp_delta' columns are found in subquery SELECT : {}. Only one is supported",
                        select_expression_list->formatForErrorMessage());

                found_delta_col = true;
            }
            else if (id->name().ends_with(ProtonConsts::RESERVED_DELTA_FLAG))
                throw Exception(
                    ErrorCodes::UNKNOWN_IDENTIFIER,
                    "Unkown identifier '{}' in select list '{}'",
                    id->formatForErrorMessage(),
                    select_expression_list->formatForErrorMessage());
        }
        /// For one table, `t.*` <=> `*`
        else if ((selected_col->as<ASTAsterisk>() || (selected_col->as<ASTQualifiedAsterisk>() && tables_with_columns.size() == 1)))
        {
            if (!asterisk_include_delta)
                continue;

            if (found_delta_col)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Multiple '_tp_delta' columns are found in subquery SELECT : {}. Only one is supported",
                    select_expression_list->formatForErrorMessage());

            found_delta_col = true;
        }
    }

    if (!found_delta_col)
    {
        if (is_subquery)
            /// Need add delta if _tp_delta is not present and the \param select_query is a subquery (not top level select)
            select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));
        else
            throw Exception(ErrorCodes::INCORRECT_QUERY, "The query with changelog output requires selecting the `_tp_delta` column explicitly");
    }

    if (add_new_required_result_columns)
        new_required_result_column_names.push_back(ProtonConsts::RESERVED_DELTA_FLAG);
}

}
}

