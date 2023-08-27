#pragma once

#include <Core/Streaming/DataStreamSemantic.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>

namespace DB
{

struct SelectQueryInfo;

namespace Streaming
{
/// Add _tp_delta column for the current select query
/// Don't go into subquery
/// SELECT count(), max(i) FROM (SELECT i FROM versioned_kv / changelog_kv / changelog) =>
/// SELECT count(), max(i) FROM (SELECT i, _tp_delta FROM versioned_kv / changelog_kv / changelog)
/// SELECT count(), max(i) FROM (SELECT i + 1 as i FROM (SELECT i FROM versioned_kv / changelog_kv / changelog)) =>
/// SELECT count(), max(i) FROM (SELECT i + 1 AS i, _tp_delta FROM (SELECT i, _tp_delta FROM versioned_kv / changelog_kv / changelog))
/// SELECT count() FROM versioned_kv / changelog_kv / changelog =>
/// SELECT count(), max(i) FROM (SELECT i, _tp_delta FROM versioned_kv / changelog_kv / changelog)
/// SELECT i, ii FROM lhs_vk INNER JOIN rhs_vk ON k = kk =>
/// SELECT i, ii FROM (SELECT i, k, _tp_delta FROM lhs_vk) as lhs_vk INNER JOIN (SELECT ii, kk, _tp_delta FROM rhs_vk) as rhs_vk ON k = kk =>
/// SELECT i, ii FROM (SELECT i, k FROM lhs_vk) as lhs_vk INNER JOIN (SELECT ii, kk FROM rhs_vk) ON k = kk =>
/// SELECT i, ii FROM (SELECT i, k, _tp_delta FROM lhs_vk) as lhs_vk INNER JOIN (SELECT ii, kk, _tp_delta FROM rhs_vk) ON k = kk
/// SELECT max(i), min(ii) FROM lhs_vk INNER JOIN rhs_vk ON k = kk =>
/// SELECT max(i), min(ii) FROM (SELECT i, k, _tp_delta FROM lhs_vk) as lhs_vk INNER JOIN (SELECT ii, kk, _tp_delta FROM rhs_vk) as rhs_vk ON k = kk
class ChangelogQueryVisitorMatcher
{
public:
    ChangelogQueryVisitorMatcher(
        DataStreamSemanticPair data_stream_semantic_pair_,
        const TablesWithColumns & tables_with_columns_,
        bool add_new_required_result_columns_,
        bool is_subquery_,
        const SelectQueryInfo & query_info_)
        : data_stream_semantic_pair(data_stream_semantic_pair_)
        , tables_with_columns(tables_with_columns_)
        , add_new_required_result_columns(add_new_required_result_columns_)
        , is_subquery(is_subquery_)
        , query_info(query_info_)
    {
    }

    using TypeToVisit = ASTSelectQuery;
    using Matcher = OneTypeMatcher<ChangelogQueryVisitorMatcher, NeedChild::none>;
    using Visitor = InDepthNodeVisitor<Matcher, true>;

    void visit(ASTSelectQuery & node, ASTPtr &);

    bool queryIsHardRewritten() const noexcept { return hard_rewritten; }

    auto && newRequiredResultColumnNames() { return std::move(new_required_result_column_names); }

private:
    void addDeltaColumn(ASTSelectQuery & select_query, bool asterisk_include_delta);

    Names new_required_result_column_names;

    DataStreamSemanticPair data_stream_semantic_pair;
    const TablesWithColumns & tables_with_columns;
    bool add_new_required_result_columns;
    bool is_subquery;
    const SelectQueryInfo & query_info;
    bool hard_rewritten = false;
};

using ChangelogQueryVisitor = ChangelogQueryVisitorMatcher::Visitor;

ASTPtr makeTemporaryDeltaColumn();
void rewriteTemporaryDeltaColumnInSelectQuery(ASTSelectQuery & select_query, bool emit_changelog);
}
}
