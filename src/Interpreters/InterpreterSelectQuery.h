#pragma once

#include <memory>

#include <Core/QueryProcessingStage.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableLockHolder.h>
#include <QueryPipeline/Pipe.h>

#include <Columns/FilterDescription.h>

/// proton: starts.
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Interpreters/Streaming/WindowCommon.h>
/// proton: ends.

namespace Poco
{
class Logger;
}

namespace DB
{

class SubqueryForSet;
class InterpreterSelectWithUnionQuery;
class Context;
class QueryPlan;
class JoinedTables;

struct GroupingSetsParams;
using GroupingSetsParamsList = std::vector<GroupingSetsParams>;

struct TreeRewriterResult;
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

/** Interprets the SELECT query. Returns the stream of blocks with the results of the query before `to_stage` stage.
  */
class InterpreterSelectQuery : public IInterpreterUnionOrSelectQuery
{
public:
    /**
     * query_ptr
     * - A query AST to interpret.
     *
     * required_result_column_names
     * - don't calculate all columns except the specified ones from the query
     *  - it is used to remove calculation (and reading) of unnecessary columns from subqueries.
     *   empty means - use all columns.
     */

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextMutablePtr & context_,
        const SelectQueryOptions &,
        const Names & required_result_column_names_ = Names{});

    /// Read data not from the table specified in the query, but from the prepared pipe `input`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        Pipe input_pipe_,
        const SelectQueryOptions & = {});

    /// Read data not from the table specified in the query, but from the specified `storage_`.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const StoragePtr & storage_,
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        const SelectQueryOptions & = {});

    /// Reuse existing prepared_sets for another pass of analysis. It's used for projection.
    /// TODO: Find a general way of sharing sets among different interpreters, such as subqueries.
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        const SelectQueryOptions &,
        PreparedSetsPtr prepared_sets_);

    ~InterpreterSelectQuery() override;

    /// Execute a query. Get the stream of blocks to read.
    BlockIO execute() override;

    /// Builds QueryPlan for current query.
    virtual void buildQueryPlan(QueryPlan & query_plan) override;

    bool ignoreLimits() const override { return options.ignore_limits; }
    bool ignoreQuota() const override { return options.ignore_quota; }

    virtual void ignoreWithTotals() override;

    ASTPtr getQuery() const { return query_ptr; }

    const SelectQueryInfo & getQueryInfo() const { return query_info; }

    SelectQueryExpressionAnalyzer * getQueryAnalyzer() const { return query_analyzer.get(); }

    const ExpressionAnalysisResult & getAnalysisResult() const { return analysis_result; }

    const Names & getRequiredColumns() const { return required_columns; }

    /// proton: starts
    bool hasAggregation() const override { return query_analyzer->hasAggregation(); }
    bool isStreamingQuery() const override;
    Streaming::DataStreamSemanticEx getDataStreamSemantic() const override { return data_stream_semantic_pair.output_data_stream_semantic; }
    std::set<String> getGroupByColumns() const override;
    bool hasStreamingWindowFunc() const override;
    Streaming::WindowType windowType() const;
    bool hasStreamingGlobalAggregation() const override;
    /// proton: ends

    static void addEmptySourceToQueryPlan(
        QueryPlan & query_plan, const Block & source_header, const SelectQueryInfo & query_info, const ContextPtr & context_);

    Names getRequiredColumns() { return required_columns; }

    bool supportsTransactions() const override { return true; }

private:
    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        PreparedSetsPtr prepared_sets_ = nullptr);

    InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextMutablePtr & context_,
        std::optional<Pipe> input_pipe,
        const StoragePtr & storage_,
        const SelectQueryOptions &,
        const Names & required_result_column_names = {},
        const StorageMetadataPtr & metadata_snapshot_ = nullptr,
        PreparedSetsPtr prepared_sets_ = nullptr);

    ASTSelectQuery & getSelectQuery() { return query_ptr->as<ASTSelectQuery &>(); }

    void addPrewhereAliasActions();
    bool shouldMoveToPrewhere();

    Block getSampleBlockImpl();

    void executeImpl(QueryPlan & query_plan, std::optional<Pipe> prepared_pipe);

    /// Different stages of query execution.
    void executeFetchColumns(QueryProcessingStage::Enum processing_stage, QueryPlan & query_plan);
    void executeWhere(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter);
    void executeAggregation(
        QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, bool final, InputOrderInfoPtr group_by_info);
    void executeMergeAggregated(QueryPlan & query_plan, bool overflow_row, bool final, bool has_grouping_sets);
    void executeTotalsAndHaving(QueryPlan & query_plan, bool has_having, const ActionsDAGPtr & expression, bool remove_filter, bool overflow_row, bool final);
    void executeHaving(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter);
    static void executeExpression(QueryPlan & query_plan, const ActionsDAGPtr & expression, const std::string & description);
    /// FIXME should go through ActionsDAG to behave as a proper function
    void executeWindow(QueryPlan & query_plan);
    void executeOrder(QueryPlan & query_plan, InputOrderInfoPtr sorting_info);
    void executeOrderOptimized(QueryPlan & query_plan, InputOrderInfoPtr sorting_info, UInt64 limit, SortDescription & output_order_descr);
    void executeWithFill(QueryPlan & query_plan);
    void executeMergeSorted(QueryPlan & query_plan, const std::string & description);
    void executePreLimit(QueryPlan & query_plan, bool do_not_skip_offset);
    void executeLimitBy(QueryPlan & query_plan);
    void executeLimit(QueryPlan & query_plan);
    void executeOffset(QueryPlan & query_plan);
    static void executeProjection(QueryPlan & query_plan, const ActionsDAGPtr & expression);
    void executeDistinct(QueryPlan & query_plan, bool before_order, Names columns, bool pre_distinct);
    void executeExtremes(QueryPlan & query_plan);
    void executeSubqueriesInSetsAndJoins(QueryPlan & query_plan);

    String generateFilterActions(ActionsDAGPtr & actions, const Names & prerequisite_columns = {}) const;

    /// proton: starts
    void executeLightShuffling(QueryPlan & query_plan);
    void executeStreamingWindow(QueryPlan & query_plan);
    void executeStreamingOrder(QueryPlan & query_plan);
    void executeStreamingAggregation(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, bool final);
    void executeStreamingPreLimit(QueryPlan & query_plan, bool do_not_skip_offset);
    void executeStreamingLimit(QueryPlan & query_plan);
    void executeStreamingOffset(QueryPlan & query_plan);
    void finalCheckAndOptimizeForStreamingQuery();
    bool shouldKeepAggregateState() const;
    void buildShufflingQueryPlan(QueryPlan & query_plan);
    void buildWatermarkQueryPlan(QueryPlan & query_plan);
    void buildStreamingProcessingQueryPlanBeforeJoin(QueryPlan & query_plan);
    void buildStreamingProcessingQueryPlanAfterJoin(QueryPlan & query_plan);
    void checkEmitVersion();
    void handleSeekToSetting();
    void analyzeEventPredicateAsSeekTo(const JoinedTables & joined_tables);
    void checkAndPrepareStreamingFunctions();
    void checkUDA();

    void resolveDataStreamSemantic(const JoinedTables & joined_tables);
    /// proton: ends

    enum class Modificator
    {
        ROLLUP = 0,
        CUBE = 1,
    };

    void executeRollupOrCube(QueryPlan & query_plan, Modificator modificator);

    /** If there is a SETTINGS section in the SELECT query, then apply settings from it.
      *
      * Section SETTINGS - settings for a specific query.
      * Normally, the settings can be passed in other ways, not inside the query.
      * But the use of this section is justified if you need to set the settings for one subquery.
      */
    void initSettings();

    TreeRewriterResultPtr syntax_analyzer_result;
    std::unique_ptr<SelectQueryExpressionAnalyzer> query_analyzer;
    SelectQueryInfo query_info;

    /// Is calculated in getSampleBlock. Is used later in readImpl.
    ExpressionAnalysisResult analysis_result;
    /// For row-level security.
    ASTPtr row_policy_filter;
    FilterDAGInfoPtr filter_info;

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

    /// List of columns to read to execute the query.
    Names required_columns;
    /// Structure of query source (table, subquery, etc).
    Block source_header;

    /// proton: starts
    /// A copy of required_columns before adding the additional ones for streaming processing
    struct StreamingSelectAnalysisContext
    {
    };

    bool emit_version = false;
    bool has_user_defined_emit_strategy = false;
    /// Bools to tell the query properties of the `current layer` of SELECT.
    bool current_select_has_aggregates = false;
    std::optional<std::pair<JoinKind, JoinStrictness>> current_select_join_kind_and_strictness; /// Which implies having join if have value
    mutable std::optional<bool> is_streaming_query;
    bool shuffled_before_join = false;
    bool light_shuffled = false;

    Streaming::WatermarkEmitMode watermark_emit_mode;

    /// Overall data stream semantic defines the output semantic of the current layer of SELECT
    Streaming::DataStreamSemanticPair data_stream_semantic_pair;
    /// proton: ends

    /// Actions to calculate ALIAS if required.
    ActionsDAGPtr alias_actions;

    /// The subquery interpreter, if the subquery
    std::unique_ptr<InterpreterSelectWithUnionQuery> interpreter_subquery;

    /// Table from where to read data, if not subquery.
    StoragePtr storage;
    StorageID table_id = StorageID::createEmpty(); /// Will be initialized if storage is not nullptr
    TableLockHolder table_lock;

    /// Used when we read from prepared input, not table or subquery.
    std::optional<Pipe> input_pipe;

    Poco::Logger * log;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;

    /// Reuse already built sets for multiple passes of analysis, possibly across interpreters.
    PreparedSetsPtr prepared_sets;
};

}
