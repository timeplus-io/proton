#pragma once

#include <Core/Joins.h>
#include <Parsers/IAST_fwd.h>

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
std::optional<std::pair<JoinKind, JoinStrictness>>
analyzeJoinKindAndStrictness(const ASTSelectQuery & select_query, JoinStrictness default_strictness);
}
}