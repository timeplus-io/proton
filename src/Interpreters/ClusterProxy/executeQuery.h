#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST.h>

namespace DB
{

struct Settings;
struct DistributedSettings;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;
struct SelectQueryInfo;

class Pipe;
class QueryPlan;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct StorageID;

namespace ClusterProxy
{

class SelectStreamFactory;

/// Update settings for Distributed query.
///
/// - Removes different restrictions (like max_concurrent_queries_for_user, max_memory_usage_for_user, etc.)
///   (but only if cluster does not have secret, since if it has, the user is the same)
/// - Update some settings depends on force_optimize_skip_unused_shards and:
///   - force_optimize_skip_unused_shards_nesting
///   - optimize_skip_unused_shards_nesting
///
/// @return new Context with adjusted settings
ContextMutablePtr
updateSettingsForCluster(const Cluster & cluster, ContextPtr context, const Settings & settings, const StorageID & main_table);

using AdditionalShardFilterGenerator = std::function<ASTPtr(uint64_t)>;

/// Execute a distributed query, creating a query plan, from which the query pipeline can be built.
/// `stream_factory` object encapsulates the logic of creating plans for a different type of query
/// (currently SELECT, DESCRIBE).
void executeQuery(
    QueryPlan & query_plan,
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    LoggerPtr log,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const DistributedSettings & distributed_settings,
    AdditionalShardFilterGenerator shard_filter_generator,
    bool is_remote_function,
    /// proton: starts.
    bool is_streaming = false,
    bool read_concat = false,
    std::optional<UInt32> shard_to_read = {});
/// proton: ends.

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    const ASTPtr & query_ast,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ClusterPtr & not_optimized_cluster,
    bool is_streaming,
    bool parallelized_to_all_replicas);

}

}
