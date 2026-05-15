#pragma once

/// Internal-only declarations for PruneShards.cpp helpers exposed for unit testing.
/// Not part of the public Storages API; do not include from production code outside
/// of PruneShards.cpp and the matching gtest.

#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST_fwd.h>

namespace DB::Internal
{

struct RewriteInSubqueriesForShardPruningResult
{
    /// Set only when an empty IN-subquery is reached purely through `and(...)` ancestors,
    /// so its "always false" value actually propagates to the root predicate.
    bool has_empty_subquery_in_conjunctive_position = false;
};

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
    size_t limit,
    RewriteInSubqueriesForShardPruningResult & result,
    bool in_conjunctive_position);

}
