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
        auto rewrite = [&](ASTPtr & query_element, DataStreamSemantic data_stream_semantic) {
            auto & table_element = query_element->as<ASTTablesInSelectQueryElement &>();
            auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

            /// If table expression is a subquery, it shall already rewritten to include
            /// the _tp_delta column
            if (!table_expr.subquery)
            {
                bool need_rewrite = (isChangelogDataStream(data_stream_semantic) || isChangelogKeyedDataStream(data_stream_semantic))
                    && query_info.changelog_tracking_changes;
                need_rewrite |= isVersionedKeyedDataStream(data_stream_semantic) && query_info.versioned_kv_tracking_changes;

                if (need_rewrite)
                {
                    rewriteAsSubquery(table_expr);
                    hard_rewritten = true;
                }
            }
        };

        for (size_t i = 0; auto & query_element : tables->children)
            rewrite(query_element, tables_with_columns[i].output_data_stream_semantic);

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

        if (data_stream_semantic_pair.isChangelogOutput())
        {
            /// For changelog/changelog_kv, the `*` include `_tp_delta`
            /// For versioned_kv, the `*` didn't include `_tp_delta`
            bool asterisk_include_delta = isChangelogDataStream(tables_with_columns.front().output_data_stream_semantic)
                || isChangelogKeyedDataStream(tables_with_columns.front().output_data_stream_semantic);
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
    [[maybe_unused]] bool has_asterisk = false;
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
            has_asterisk = true;
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

    /// Here are two scenarios, need add delta
    /// 1) The @p select_query is a subquery, and _tp_delta is not present
    if (is_subquery && !found_delta_col)
        select_expression_list->children.emplace_back(std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));

    /// 2) The @p select_query is a top query, and _tp_delta is not present, and the select list has `*` (The `*` should be expanded in `TranslateQualifiedNamesVisitor` or `JoinToSubqueryTransformVisitor` (Skipped)
    assert(!(!is_subquery && !found_delta_col && has_asterisk));

    if (add_new_required_result_columns)
        new_required_result_column_names.push_back(ProtonConsts::RESERVED_DELTA_FLAG);
}

ASTPtr makeTemporaryDeltaColumn()
{
    auto tmp_delta_col = std::make_shared<ASTLiteral>(Int8(1));
    tmp_delta_col->setAlias(ProtonConsts::RESERVED_DELTA_FLAG);
    return tmp_delta_col;
}

void rewriteTemporaryDeltaColumnInSelectQuery(ASTSelectQuery & select_query, bool emit_changelog)
{
    const auto select_expression_list = select_query.select();
    for (auto iter = select_expression_list->children.begin(); iter != select_expression_list->children.end();)
    {
        if ((*iter)->as<ASTLiteral>() && (*iter)->tryGetAlias() == ProtonConsts::RESERVED_DELTA_FLAG)
        {
            if (emit_changelog)
            {
                /// Rewrite temporary column `1 as _tp_delta` to `_tp_delta`
                *iter = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG);
                ++iter;
            }
            else
            {
                /// Remove temporary column `1 as _tp_delta`
                iter = select_expression_list->children.erase(iter);
            }
        }
        else
            ++iter;
    }
}
}
}
