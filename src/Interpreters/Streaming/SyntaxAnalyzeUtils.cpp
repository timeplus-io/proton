#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/Streaming/SyntaxAnalyzeUtils.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace ErrorCodes
{
extern const int EXPECTED_ALL_OR_ANY;
}

namespace Streaming
{
std::pair<bool, bool> analyzeSelectQueryForJoinOrAggregates(const ASTPtr & select_query)
{
    std::pair<bool, bool> result;

    const auto * local_select_query = select_query->as<ASTSelectQuery>();
    if (!local_select_query)
        throw Exception("Select query is expected.", ErrorCodes::LOGICAL_ERROR);

    const auto & tables = local_select_query->tables();
    if (tables && tables->children.size() >= 2)
        result.first = true;

    if (local_select_query->groupBy())
        result.second = true;

    if (result.first && result.second)
        return result;

    /// Check if it is global aggregate without group by
    /// select count(*) from stream;
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(select_query);

    result.second = !data.aggregates.empty() || !data.aggregate_overs.empty();
    return result;
}

bool selectQueryHasJoinOrAggregates(const ASTPtr & select_query)
{
    auto result = analyzeSelectQueryForJoinOrAggregates(select_query);
    return result.first || result.second;
}

std::optional<std::pair<JoinKind, JoinStrictness>>
analyzeJoinKindAndStrictness(const ASTSelectQuery & select_query, JoinStrictness default_strictness)
{
    const auto & tables = select_query.tables();
    if (!tables)
        return {};

    const ASTTablesInSelectQueryElement * node = nullptr;
    for (auto riter = tables->children.rbegin(); riter != tables->children.rend(); ++riter)
    {
        const auto & tables_element = (*riter)->as<ASTTablesInSelectQueryElement &>();
        if (tables_element.table_join)
        {
            node = &tables_element;
            break;
        }
    }

    if (!node)
        return {};

    const auto & table_join = node->table_join->as<const ASTTableJoin &>();

    JoinKind kind = table_join.kind;
    JoinStrictness strictness = table_join.strictness;
    if (strictness == JoinStrictness::Unspecified && kind != JoinKind::Cross)
    {
        if (default_strictness == JoinStrictness::Any)
            strictness = JoinStrictness::Any;
        else if (default_strictness == JoinStrictness::All)
            strictness = JoinStrictness::All;
        else
            throw Exception(
                "Expected ANY or ALL in JOIN section, because setting (join_default_strictness) is empty",
                DB::ErrorCodes::EXPECTED_ALL_OR_ANY);
    }

    return std::make_pair(kind, strictness);
}
}
}