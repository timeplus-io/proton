#pragma once

#include <Interpreters/PreparedSets.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Core/SortDescription.h>
#include <Core/Names.h>
#include <Storages/ProjectionsDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <memory>

/// proton: starts.
#include <Storages/Streaming/SeekToInfo.h>
/// proton: ends.

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

struct FilterInfo;
using FilterInfoPtr = std::shared_ptr<FilterInfo>;

struct FilterDAGInfo;
using FilterDAGInfoPtr = std::shared_ptr<FilterDAGInfo>;

struct InputOrderInfo;
using InputOrderInfoPtr = std::shared_ptr<const InputOrderInfo>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

class ReadInOrderOptimizer;
using ReadInOrderOptimizerPtr = std::shared_ptr<const ReadInOrderOptimizer>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

struct MergeTreeDataSelectAnalysisResult;
using MergeTreeDataSelectAnalysisResultPtr = std::shared_ptr<MergeTreeDataSelectAnalysisResult>;

/// proton: starts.
namespace Streaming
{
struct WindowParams;
using WindowParamsPtr = std::shared_ptr<WindowParams>;
}
/// proton: ends.

struct PrewhereInfo
{
    /// Actions for row level security filter. Applied separately before prewhere_actions.
    /// This actions are separate because prewhere condition should not be executed over filtered rows.
    ActionsDAGPtr row_level_filter;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ActionsDAGPtr prewhere_actions;
    String row_level_column_name;
    String prewhere_column_name;
    bool remove_prewhere_column = false;
    bool need_filter = false;

    PrewhereInfo() = default;
    explicit PrewhereInfo(ActionsDAGPtr prewhere_actions_, String prewhere_column_name_)
            : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}

    std::string dump() const;
};

/// Helper struct to store all the information about the filter expression.
struct FilterInfo
{
    ExpressionActionsPtr alias_actions;
    ExpressionActionsPtr actions;
    String column_name;
    bool do_remove_column = false;
};

/// Same as FilterInfo, but with ActionsDAG.
struct FilterDAGInfo
{
    ActionsDAGPtr actions;
    String column_name;
    bool do_remove_column = false;

    std::string dump() const;
};

struct InputOrderInfo
{
    SortDescription order_key_prefix_descr;
    int direction;
    UInt64 limit;

    InputOrderInfo(const SortDescription & order_key_prefix_descr_, int direction_, UInt64 limit_)
        : order_key_prefix_descr(order_key_prefix_descr_), direction(direction_), limit(limit_) {}

    bool operator ==(const InputOrderInfo & other) const
    {
        return order_key_prefix_descr == other.order_key_prefix_descr && direction == other.direction;
    }

    bool operator !=(const InputOrderInfo & other) const { return !(*this == other); }
};

class IMergeTreeDataPart;

using ManyExpressionActions = std::vector<ExpressionActionsPtr>;

// The projection selected to execute current query
struct ProjectionCandidate
{
    ProjectionDescriptionRawPtr desc{};
    PrewhereInfoPtr prewhere_info;
    ActionsDAGPtr before_where;
    String where_column_name;
    bool remove_where_filter = false;
    ActionsDAGPtr before_aggregation;
    Names required_columns;
    NamesAndTypesList aggregation_keys;
    AggregateDescriptions aggregate_descriptions;
    bool aggregate_overflow_row = false;
    bool aggregate_final = false;
    bool complete = false;
    ReadInOrderOptimizerPtr order_optimizer;
    InputOrderInfoPtr input_order_info;
    ManyExpressionActions group_by_elements_actions;
    SortDescription group_by_elements_order_descr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;
};

/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    SelectQueryInfo()
        : prepared_sets(std::make_shared<PreparedSets>())
    {}

    ASTPtr query;
    ASTPtr view_query; /// Optimized VIEW query
    ASTPtr original_query; /// Unmodified query for projection analysis
    /// proton: starts.
    ASTPtr optimized_proxy_stream_query;  /// Optimized nested query in StreamProxy
    /// proton: ends.

    std::shared_ptr<const StorageLimitsList> storage_limits;

    /// Cluster for the query.
    ClusterPtr cluster;
    /// Optimized cluster for the query.
    /// In case of optimize_skip_unused_shards it may differs from original cluster.
    ///
    /// Configured in StorageDistributed::getQueryProcessingStage()
    ClusterPtr optimized_cluster;

    TreeRewriterResultPtr syntax_analyzer_result;

    PrewhereInfoPtr prewhere_info;

    ReadInOrderOptimizerPtr order_optimizer;
    /// Can be modified while reading from storage
    InputOrderInfoPtr input_order_info;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSetsPtr prepared_sets;

    /// Cached value of ExpressionAnalysisResult
    bool has_window = false;

    /// proton: starts.
    SeekToInfoPtr seek_to_info; /// Rewind info for left streaming store in streaming query
    SeekToInfoPtr seek_to_info_of_right_stream; /// Rewind info for right streaming store in streaming query

    /// if is true, means need sort backfill input by event time (ascending)
    bool require_in_order_backfill = false;

    Streaming::WindowParamsPtr streaming_window_params;

    bool left_input_tracking_changes = false;
    bool right_input_tracking_changes = false;
    bool force_emit_changelog = false;
    std::optional<bool> changelog_query_drop_late_rows = true;

    bool has_aggregate_over = false;
    bool has_non_aggregate_over = false;
    bool has_javascript_uda = false; /// Used to guide query concurrency
    Names partition_by_keys;

    bool hasPartitionByKeys() const noexcept { return !partition_by_keys.empty(); }
    bool trackingChanges() const noexcept { return left_input_tracking_changes || right_input_tracking_changes; }
    /// proton: ends.

    ClusterPtr getCluster() const { return !optimized_cluster ? cluster : optimized_cluster; }

    /// If not null, it means we choose a projection to execute current query.
    std::optional<ProjectionCandidate> projection;
    bool ignore_projections = false;
    bool is_projection_query = false;
    bool merge_tree_empty_result = false;
    bool settings_limit_offset_done = false;
    Block minmax_count_projection_block;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_select_result_ptr;

    InputOrderInfoPtr getInputOrderInfo() const
    {
        return input_order_info ? input_order_info : (projection ? projection->input_order_info : nullptr);
    }
};
}
