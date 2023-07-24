#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Joins.h>

namespace DB
{
class ASTSelectQuery;

namespace Streaming
{
/// Syntactically analyze if the top level select query has join or aggregates
/// Don't delve into subquery recursively
/// Return {true, true} if either the top select has join and aggregates
std::pair<bool, bool> analyzeSelectQueryForJoinOrAggregates(const ASTPtr & select_query);
bool selectQueryHasJoinOrAggregates(const ASTPtr & select_query);

/// Syntactically analyze the join strictness
std::optional<JoinStrictness> analyzeJoinStrictness(const ASTSelectQuery & select_query, JoinStrictness default_strictness);
}
}