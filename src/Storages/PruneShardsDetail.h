#pragma once

/// Internal-only declarations for PruneShards.cpp helpers exposed for unit testing.
/// Not part of the public Storages API; do not include from production code outside
/// of PruneShards.cpp and the matching gtest.

#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST_fwd.h>

#include <memory>

namespace DB
{

class QueryPlan;

namespace detail
{

struct RewriteInSubqueriesForShardPruningResult
{
    /// Set only when an empty IN-subquery is reached purely through `and(...)` ancestors,
    /// so its "always false" value actually propagates to the root predicate.
    bool has_empty_subquery_in_conjunctive_position = false;
};

/// Shard-pruning-only helper exposed for focused tests; mirrors the streaming-plan
/// guard before early IN-subquery materialization.
bool canMaterializeSubqueryForShardPruning(const QueryPlan & subquery_plan);

/// Shard-pruning-only helper exposed for focused tests; builds the early
/// IN-subquery plan with the outer query's current subquery depth.
std::unique_ptr<QueryPlan> buildSubqueryPlanForShardPruning(
    const ASTPtr & subquery, const ContextPtr & context, size_t subquery_depth);

/// Shard-pruning-only helper exposed for focused tests; registers an already-planned
/// bounded subquery with explicit set collection capped to the pruning limit.
FutureSetPtr addSubqueryPlanForShardPruning(
    const PreparedSetsPtr & prepared_sets,
    const PreparedSets::Hash & set_key,
    std::unique_ptr<QueryPlan> subquery_plan,
    const ContextPtr & context,
    size_t limit);

/// Walks `node` and rewrites every `in(col, subquery)` whose set is already prepared,
/// replacing the subquery with a literal tuple of values when the set is small enough.
/// Sets `result.has_empty_subquery_in_conjunctive_position` only when an empty IN-subquery
/// is reached purely through `and(...)` ancestors -- otherwise the surrounding logic may
/// still evaluate to true for some rows, and short-circuiting shard pruning would lose them.
/// Returns true iff `node` was modified in place.
bool rewriteInSubqueriesForShardPruning(
    ASTPtr & node,
    const PreparedSetsPtr & prepared_sets,
    const ContextPtr & context,
    size_t subquery_depth,
    size_t limit,
    RewriteInSubqueriesForShardPruningResult & result,
    bool in_conjunctive_position);

}

}
