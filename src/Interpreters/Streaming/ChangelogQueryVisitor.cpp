#include <Interpreters/Streaming/ChangelogQueryVisitor.h>
/// #include <Interpreters/RequiredSourceColumnsVisitor.h>

#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ALIAS_REQUIRED;
extern const int UNKNOWN_IDENTIFIER;
extern const int NOT_IMPLEMENTED;
}

namespace
{
ASTPtr rewriteAsSubqueryImpl(ASTTableExpression & table_expr)
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
                    rewriteAsSubquery(table_expr);
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

void ChangelogQueryVisitorMatcher::rewriteAsSubquery(ASTTableExpression & table_expr)
{
    /// SELECT vk.k, vk2.j FROM vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// SELECT vk.k, vk2.j FROM (SELECT * FROM vk) AS vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// Having alias
    /// SELECT vk.k, vk2.j FROM changelog(vk) AS vk JOIN vk2 ON vk.k = vk2.k2; =>
    /// SELECT vk.k, vk2.j FROM (SELECT * FROM changelog(vk)) AS vk JOIN vk2 ON vk.k = vk2.k2; =>

    /// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
    table_expr.subquery = rewriteAsSubqueryImpl(table_expr);
    table_expr.children.clear();
    table_expr.database_and_table_name = nullptr;
    table_expr.table_function = nullptr;
    table_expr.children.push_back(table_expr.subquery);
    hard_rewritten = true;
}

void ChangelogQueryVisitorMatcher::addDeltaColumn(ASTSelectQuery & select_query, bool asterisk_include_delta)
{
    /// Append `_tp_delta` to the select projection
    /// Parent select has join or aggregates, and current select is just a flat select
    /// add _tp_delta to select list if _tp_delta is not present.
    /// SELECT i, k FROM vk => SELECT i, k, _tp_delta FROM vk
    const auto select_expression_list = select_query.select();

    bool found_delta_col = false;
    bool has_asterisk = false;
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
