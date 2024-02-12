#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeInterval.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <AggregateFunctions/AggregateFunctionCount.h>

#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/UnnestSubqueryVisitor.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/StorageView.h>
#include <Storages/StorageDistributed.h>

#include <Functions/IFunction.h>
#include <Core/Field.h>
#include <base/types.h>
#include <base/sort.h>
#include <Columns/Collator.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/IJoin.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/map.h>
#include <Common/scope_guard_safe.h>

/// proton: starts
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Interpreters/Streaming/ChangelogQueryVisitor.h>
#include <Interpreters/Streaming/EmitInterpreter.h>
#include <Interpreters/Streaming/EventPredicateVisitor.h>
#include <Interpreters/Streaming/IHashJoin.h>
#include <Interpreters/Streaming/PartitionByVisitor.h>
#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Interpreters/Streaming/SubstituteStreamingFunction.h>
#include <Interpreters/Streaming/SyntaxAnalyzeUtils.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Processors/QueryPlan/LightShufflingStep.h>
#include <Processors/QueryPlan/Streaming/AggregatingStep.h>
#include <Processors/QueryPlan/Streaming/AggregatingStepWithSubstream.h>
#include <Processors/QueryPlan/Streaming/JoinStep.h>
#include <Processors/QueryPlan/Streaming/LimitStep.h>
#include <Processors/QueryPlan/Streaming/OffsetStep.h>
#include <Processors/QueryPlan/Streaming/ReplayStreamStep.h>
#include <Processors/QueryPlan/Streaming/ShufflingStep.h>
#include <Processors/QueryPlan/Streaming/SortingStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStepWithSubstream.h>
#include <Processors/QueryPlan/Streaming/WindowStep.h>
#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/Streaming/WatermarkStamper.h>
#include <Storages/Streaming/ProxyStream.h>
#include <Storages/Streaming/StorageStream.h>
#include <Storages/Streaming/storageUtil.h>
#include <Common/ProtonCommon.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int ILLEGAL_FINAL;
    extern const int ILLEGAL_PREWHERE;
    extern const int TOO_MANY_COLUMNS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int INVALID_WITH_FILL_EXPRESSION;
    extern const int ACCESS_DENIED;

    /// proton: starts
    extern const int WINDOW_COLUMN_NOT_REFERENCED;
    extern const int INVALID_STREAMING_FUNC_DESC;
    extern const int MISSING_GROUP_BY;
    extern const int UNSUPPORTED;
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int UDA_NOT_APPLICABLE;
    /// proton: ends
}

/// proton: starts.
namespace
{
/// Add where expression: <event_time> >= to_datetime64(utc_ms/1000, 3, 'UTC')
void addEventTimePredicate(ASTSelectQuery & select, Int64 utc_ms)
{
    auto greater = makeASTFunction(
        "greater_or_equals",
        std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME),
        makeASTFunction(
            "to_datetime64",
            makeASTFunction("divide", std::make_shared<ASTLiteral>(utc_ms), std::make_shared<ASTLiteral>(1000)),
            std::make_shared<ASTLiteral>(UInt64(3)),
            std::make_shared<ASTLiteral>("UTC")));

    if (auto where = select.where())
        select.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", greater, where));
    else
        select.setExpression(ASTSelectQuery::Expression::WHERE, greater);
}

std::vector<size_t> keyPositions(const Block & header, const Names & key_columns)
{
    std::vector<size_t> key_positions;
    key_positions.reserve(key_columns.size());
    for (const auto & key : key_columns)
        key_positions.emplace_back(header.getPositionByName(key));
    return key_positions;
}

/// Requires: 1) no window function 2) has aggregation
bool hasGlobalAggregationInQuery(const ASTPtr & query, const ASTSelectQuery & select_query, StoragePtr storage)
{
    if (storage)
    {
        if (auto * proxy = storage->as<Streaming::ProxyStream>(); proxy && proxy->getStreamingWindowFunctionDescription())
            return false;
    }

    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);
    return !data.aggregates.empty() || select_query.groupBy() != nullptr;
}

Names getShuffleByColumns(const ASTSelectQuery & query)
{
    Names shuffle_by_columns;

    if (!query.shuffleBy())
        return shuffle_by_columns;

    shuffle_by_columns.reserve(query.shuffleBy()->children.size());
    for (const auto & elem : query.shuffleBy()->children)
    {
        if (elem->as<ASTIdentifier>())
            shuffle_by_columns.push_back(elem->getColumnName());
    }

    return shuffle_by_columns;
}
}
/// proton: ends.

/// Assumes `storage` is set and the table filter (row-level security) is not empty.
String InterpreterSelectQuery::generateFilterActions(ActionsDAGPtr & actions, const Names & prerequisite_columns) const
{
    const auto & db_name = table_id.getDatabaseName();
    const auto & table_name = table_id.getTableName();

    /// TODO: implement some AST builders for this kind of stuff
    ASTPtr query_ast = std::make_shared<ASTSelectQuery>();
    auto * select_ast = query_ast->as<ASTSelectQuery>();

    select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    auto expr_list = select_ast->select();

    /// The first column is our filter expression.
    /// the row_policy_filter should be cloned, because it may be changed by TreeRewriter.
    /// which make it possible an invalid expression, although it may be valid in whole select.
    expr_list->children.push_back(row_policy_filter->clone());

    /// Keep columns that are required after the filter actions.
    for (const auto & column_str : prerequisite_columns)
    {
        ParserExpression expr_parser;
        expr_list->children.push_back(parseQuery(expr_parser, column_str, 0, context->getSettingsRef().max_parser_depth));
    }

    select_ast->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables = select_ast->tables();
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    tables->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(db_name, table_name);
    table_expr->children.push_back(table_expr->database_and_table_name);

    /// Using separate expression analyzer to prevent any possible alias injection
    auto syntax_result = TreeRewriter(context).analyzeSelect(query_ast, TreeRewriterResult({}, storage, storage_snapshot));
    SelectQueryExpressionAnalyzer analyzer(query_ast, syntax_result, context, metadata_snapshot);
    actions = analyzer.simpleSelectActions();

    auto column_name = expr_list->children.at(0)->getColumnName();
    actions->removeUnusedActions(NameSet{column_name});
    actions->projectInput(false);

    for (const auto * node : actions->getInputs())
        actions->getOutputs().push_back(node);

    return column_name;
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextPtr & context_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names_)
    : InterpreterSelectQuery(query_ptr_, context_, std::nullopt, nullptr, options_, required_result_column_names_)
{}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextMutablePtr & context_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names_)
    : InterpreterSelectQuery(query_ptr_, context_, std::nullopt, nullptr, options_, required_result_column_names_)
{}

InterpreterSelectQuery::InterpreterSelectQuery(
        const ASTPtr & query_ptr_,
        const ContextPtr & context_,
        Pipe input_pipe_,
        const SelectQueryOptions & options_)
        : InterpreterSelectQuery(query_ptr_, context_, std::move(input_pipe_), nullptr, options_.copy().noSubquery())
{}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextPtr & context_,
    const StoragePtr & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const SelectQueryOptions & options_)
    : InterpreterSelectQuery(query_ptr_, context_, std::nullopt, storage_, options_.copy().noSubquery(), {}, metadata_snapshot_)
{}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextPtr & context_,
    const SelectQueryOptions & options_,
    PreparedSetsPtr prepared_sets_)
    : InterpreterSelectQuery(
        query_ptr_, context_, std::nullopt, nullptr, options_, {}, {}, prepared_sets_)
{}

InterpreterSelectQuery::~InterpreterSelectQuery() = default;


/** There are no limits on the maximum size of the result for the subquery.
  *  Since the result of the query is not the result of the entire query.
  */
static ContextPtr getSubqueryContext(const ContextPtr & context)
{
    auto subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 0;
    subquery_settings.max_result_bytes = 0;
    /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
    subquery_settings.extremes = false;
    subquery_context->setSettings(subquery_settings);
    return subquery_context;
}

static void rewriteMultipleJoins(ASTPtr & query, const TablesWithColumns & tables, const String & database, const Settings & settings)
{
    ASTSelectQuery & select = query->as<ASTSelectQuery &>();

    Aliases aliases;
    if (ASTPtr with = select.with())
        QueryAliasesNoSubqueriesVisitor(aliases).visit(with);
    QueryAliasesNoSubqueriesVisitor(aliases).visit(select.select());

    CrossToInnerJoinVisitor::Data cross_to_inner{tables, aliases, database};
    cross_to_inner.cross_to_inner_join_rewrite = static_cast<UInt8>(std::min<UInt64>(settings.cross_to_inner_join_rewrite, 2));
    CrossToInnerJoinVisitor(cross_to_inner).visit(query);

    JoinToSubqueryTransformVisitor::Data join_to_subs_data{tables, aliases};
    JoinToSubqueryTransformVisitor(join_to_subs_data).visit(query);
}

/// Checks that the current user has the SELECT privilege.
static void checkAccessRightsForSelect(
    const ContextPtr & context,
    const StorageID & table_id,
    const StorageMetadataPtr & table_metadata,
    const TreeRewriterResult & syntax_analyzer_result)
{
    if (!syntax_analyzer_result.has_explicit_columns && table_metadata && !table_metadata->getColumns().empty())
    {
        /// For a trivial query like "SELECT count() FROM table" access is granted if at least
        /// one column is accessible.
        /// In this case just checking access for `required_columns` doesn't work correctly
        /// because `required_columns` will contain the name of a column of minimum size (see TreeRewriterResult::collectUsedColumns())
        /// which is probably not the same column as the column the current user has access to.
        auto access = context->getAccess();
        for (const auto & column : table_metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, table_id.database_name, table_id.table_name, column.name))
                return;
        }
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "{}: Not enough privileges. To execute this query it's necessary to have grant SELECT for at least one column on {}",
            context->getUserName(),
            table_id.getFullTableName());
    }

    /// General check.
    context->checkAccess(AccessType::SELECT, table_id, syntax_analyzer_result.requiredSourceColumnsForAccessCheck());
}

/// Returns true if we should ignore quotas and limits for a specified table in the system database.
static bool shouldIgnoreQuotaAndLimits(const StorageID & table_id)
{
    if (table_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
    {
        static const boost::container::flat_set<String> tables_ignoring_quota{"quotas", "quota_limits", "quota_usage", "quotas_usage", "one"};
        if (tables_ignoring_quota.contains(table_id.table_name))
            return true;
    }
    return false;
}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextPtr & context_,
    std::optional<Pipe> input_pipe_,
    const StoragePtr & storage_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names,
    const StorageMetadataPtr & metadata_snapshot_,
    PreparedSetsPtr prepared_sets_)
    : InterpreterSelectQuery(
        query_ptr_,
        Context::createCopy(context_),
        std::move(input_pipe_),
        storage_,
        options_,
        required_result_column_names,
        metadata_snapshot_,
        prepared_sets_)
{}

InterpreterSelectQuery::InterpreterSelectQuery(
    const ASTPtr & query_ptr_,
    const ContextMutablePtr & context_,
    std::optional<Pipe> input_pipe_,
    const StoragePtr & storage_,
    const SelectQueryOptions & options_,
    const Names & required_result_column_names,
    const StorageMetadataPtr & metadata_snapshot_,
    PreparedSetsPtr prepared_sets_)
    /// NOTE: the query almost always should be cloned because it will be modified during analysis.
    : IInterpreterUnionOrSelectQuery(options_.modify_inplace ? query_ptr_ : query_ptr_->clone(), context_, options_)
    , storage(storage_)
    , input_pipe(std::move(input_pipe_))
    , log(&Poco::Logger::get("InterpreterSelectQuery"))
    , metadata_snapshot(metadata_snapshot_)
    , prepared_sets(prepared_sets_)
{
    checkStackSize();

    if (!prepared_sets)
        prepared_sets = std::make_shared<PreparedSets>();

    query_info.ignore_projections = options.ignore_projections;
    query_info.is_projection_query = options.is_projection_query;
    query_info.original_query = query_ptr->clone();

    /// proton : starts.  Merge some options
    bool current_select_has_join = false;
    std::tie(current_select_has_join, current_select_has_aggregates) = Streaming::analyzeSelectQueryForJoinOrAggregates(query_ptr);

    if (current_select_has_join)
        current_select_join_kind_and_strictness
            = Streaming::analyzeJoinKindAndStrictness(getSelectQuery(), context->getSettingsRef().join_default_strictness);
    /// proton : ends

    initSettings();
    const Settings & settings = context->getSettingsRef();

    if (settings.max_subquery_depth && options.subquery_depth > settings.max_subquery_depth)
        throw Exception("Too deep subqueries. Maximum: " + settings.max_subquery_depth.toString(),
            ErrorCodes::TOO_DEEP_SUBQUERIES);

    bool has_input = input_pipe.has_value();
    if (input_pipe)
    {
        /// Read from prepared input.
        source_header = input_pipe->getHeader();
    }

    // Only propagate WITH elements to subqueries if we're not a subquery
    if (!options.is_subquery)
    {
        if (settings.enable_global_with_statement)
            ApplyWithAliasVisitor().visit(query_ptr);

        ApplyWithSubqueryVisitor().visit(query_ptr);
    }

    /// Try to eliminate subquery
    if (settings.unnest_subqueries)
    {
        UnnestSubqueryVisitorData data;
        UnnestSubqueryVisitor(data).visit(query_ptr);
    }

    /// proton: starts. Try to process the streaming query extension grammar.
    /// we need to process before table storage generation (maybe has table function)
    if (auto emit = getSelectQuery().emit())
    {
        Streaming::EmitInterpreter::handleRules(
            query_ptr, Streaming::EmitInterpreter::checkEmitAST, Streaming::EmitInterpreter::LastXRule(settings, log));

        /// Force emit changelog, for example: `select * from versioned_kv emit changelog`
        if (emit->as<ASTEmitQuery &>().stream_mode == ASTEmitQuery::StreamMode::CHANGELOG)
            query_info.force_emit_changelog = true;

        /// After handling, update setting for context.
        if (getSelectQuery().settings())
            InterpreterSetQuery(getSelectQuery().settings(), context).executeForCurrentContext();
    }
    /// proton: ends.

    JoinedTables joined_tables(getSubqueryContext(context), getSelectQuery(), options.with_all_cols);
    bool got_storage_from_query = false;

    auto clear_inits = [this]() {
        storage = nullptr;
        table_lock.reset();
        table_id = StorageID::createEmpty();
        metadata_snapshot = nullptr;
        storage_snapshot = nullptr;
    };

    auto resolve_tables_and_rewrite_join = [&got_storage_from_query, &has_input, this, &joined_tables, &clear_inits]() {
        got_storage_from_query = false;
        if (!has_input && !storage)
        {
            storage = joined_tables.getLeftTableStorage();
            got_storage_from_query = true;
        }

        if (storage)
        {
            table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef().lock_acquire_timeout);
            table_id = storage->getStorageID();
            /// proton: starts
            if (!metadata_snapshot)
            {
                if (storage->getName() == "Distributed")
                {
                    const StorageDistributed * storage_distributed = static_cast<const StorageDistributed *>(storage.get());
                    StoragePtr storage_replicated = DatabaseCatalog::instance().getTable(
                        StorageID(storage_distributed->getRemoteDatabaseName(), storage_distributed->getRemoteTableName()), context);
                    if (storage_replicated)
                        metadata_snapshot = storage_replicated->getInMemoryMetadataPtr();
                    else
                        metadata_snapshot = storage->getInMemoryMetadataPtr();
                }
                else
                {
                    metadata_snapshot = storage->getInMemoryMetadataPtr();
                }
            }
            /// proton: ends

            storage_snapshot = storage->getStorageSnapshotForQuery(metadata_snapshot, query_ptr, context);
        }

        if (has_input || !joined_tables.resolveTables())
            joined_tables.makeFakeTable(storage, metadata_snapshot, source_header);

        if (context->getCurrentTransaction() && context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
        {
            if (storage)
                checkStorageSupportsTransactionsIfNeeded(storage, context);

            for (const auto & table : joined_tables.tablesWithColumns())
            {
                if (table.table.table.empty())
                    continue;
                auto maybe_storage = DatabaseCatalog::instance().tryGetTable({table.table.database, table.table.table}, context);
                if (!maybe_storage)
                    continue;

                checkStorageSupportsTransactionsIfNeeded(storage, context);
            }
        }

        /// Rewrite JOINs
        if (!has_input && joined_tables.tablesCount() > 1)
        {
            rewriteMultipleJoins(query_ptr, joined_tables.tablesWithColumns(), context->getCurrentDatabase(), context->getSettingsRef());

            joined_tables.reset(getSelectQuery());
            joined_tables.resolveTables();

            if (storage && joined_tables.isLeftTableSubquery())
            {
                /// Rewritten with subquery. Free storage locks here.
                clear_inits();
            }
        }

        if (!has_input)
        {
            interpreter_subquery = joined_tables.makeLeftTableSubquery(options.subquery());
            if (interpreter_subquery)
                source_header = interpreter_subquery->getSampleBlock();
        }

        /// proton : starts. After resolving the tables and rewrite multiple joins
        /// we will have at most 2 tables : left table (or subquery) and right table (or subquery)
        /// It is a good time to resolve the data stream semantic of the whole query
        resolveDataStreamSemantic(joined_tables);
        /// proton : ends
    };

    resolve_tables_and_rewrite_join();

    /// proton : starts
    /// both isStreamingQuery depends on `storage / interpreterSubquery` to calculate
    /// Do this only after resolving storage since both `isStreamingQuery` depends on it
    const auto * required_result_column_names_p = &required_result_column_names;
    std::unique_ptr<Names> new_required_result_column_names;
    if (!has_input)
    {
        const auto & tables = joined_tables.tablesWithColumns();
        assert(tables.size() <= 2);

        if (isStreamingQuery() && (query_info.trackingChanges() || data_stream_semantic_pair.isChangelogOutput()))
        {
            /// A special case: global aggr over global aggr, for example:
            /// `select count() from (select count() from stream) emit changelog`
            /// The outer global aggr needs emit changelog, we shall force the nested global aggr emit changelog.
            /// Since the outer global aggr does not retain state (unless nested one emits aggregated changes)
            /// the retraction of outer global aggr will not work correctly
            if (tables.size() == 1 && data_stream_semantic_pair.isChangelogOutput()
                && hasGlobalAggregationInQuery(query_ptr, getSelectQuery(), storage))
            {
                bool force_single_subquery_input_to_emit_changelog = false;
                if (interpreter_subquery && interpreter_subquery->hasStreamingGlobalAggregation())
                    force_single_subquery_input_to_emit_changelog = true;
                else if (storage)
                {
                    auto * proxy = storage->as<Streaming::ProxyStream>();
                    force_single_subquery_input_to_emit_changelog = proxy && proxy->hasStreamingGlobalAggregation();
                }

                if (force_single_subquery_input_to_emit_changelog)
                {
                    query_info.left_input_tracking_changes = true;
                    data_stream_semantic_pair.effective_input_data_stream_semantic = Streaming::DataStreamSemantic::Changelog;
                }
            }

            /// Rewrite select query to add back _tp_delta if it is not present
            Streaming::ChangelogQueryVisitorMatcher data(
                data_stream_semantic_pair,
                tables,
                !required_result_column_names.empty(),
                options.is_subquery,
                query_info);

            Streaming::ChangelogQueryVisitor(data).visit(query_ptr);
            if (data.queryInputIsRewritten())
            {
                clear_inits();

                joined_tables.reset(getSelectQuery());
                resolve_tables_and_rewrite_join();
            }

            if (auto && new_required_columns = data.newRequiredResultColumnNames(); !new_required_columns.empty())
            {
                /// Make a copy of existing required result column names and add the new ones
                new_required_result_column_names = std::make_unique<Names>(required_result_column_names);
                for (auto & new_required_column : new_required_columns)
                {
                    if (std::find(new_required_result_column_names->begin(), new_required_result_column_names->end(), new_required_column) == new_required_result_column_names->end())
                        new_required_result_column_names->emplace_back(std::move(new_required_column));
                }

                required_result_column_names_p = new_required_result_column_names.get();
            }
        }
    }

    /// Before analyzing, handle settings seek_to
    handleSeekToSetting();
    /// proton: ends.

    joined_tables.rewriteDistributedInAndJoins(query_ptr);

    max_streams = settings.max_threads;
    ASTSelectQuery & query = getSelectQuery();

    std::shared_ptr<TableJoin> table_join = joined_tables.makeTableJoin(query);

    /// proton : starts, only allow system.* access under a special setting
    if (storage)
    {
        row_policy_filter = context->getRowPolicyFilter(table_id.getDatabaseName(), table_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

        if (table_id.getDatabaseName() == "system" && !settings._tp_internal_system_open_sesame.value)
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Have no permission to access system database");
    }
    /// proton : ends

    StorageView * view = nullptr;
    if (storage)
        view = dynamic_cast<StorageView *>(storage.get());

    auto analyze = [&] (bool try_move_to_prewhere)
    {
        /// Allow push down and other optimizations for VIEW: replace with subquery and rewrite it.
        ASTPtr view_table;
        if (view)
            view->replaceWithSubquery(getSelectQuery(), view_table, metadata_snapshot);

        TreeRewriterResult tree_rewriter_result(source_header.getNamesAndTypesList(), storage, storage_snapshot);
        tree_rewriter_result.streaming = isStreamingQuery();
        tree_rewriter_result.emit_changelog = data_stream_semantic_pair.isChangelogOutput();
        tree_rewriter_result.is_changelog_input = data_stream_semantic_pair.isChangelogInput();

        syntax_analyzer_result = TreeRewriter(context).analyzeSelect(
            query_ptr,
            std::move(tree_rewriter_result),
            options,
            joined_tables.tablesWithColumns(),
            *required_result_column_names_p,
            table_join);

        checkEmitVersion();

        /// If `optimized_proxy_stream_query` exists, skip reassign current `syntax_analyzer_result->optimized_proxy_stream_query`,
        /// since it may be nullptr after some optimizations at first time
        if (!query_info.optimized_proxy_stream_query)
            query_info.optimized_proxy_stream_query = syntax_analyzer_result->optimized_proxy_stream_query;
        /// proton: ends

        query_info.syntax_analyzer_result = syntax_analyzer_result;
        context->setDistributed(syntax_analyzer_result->is_remote_storage);

        if (storage && !query.final() && storage->needRewriteQueryWithFinal(syntax_analyzer_result->requiredSourceColumns()))
            query.setFinal();

        /// Save scalar sub queries's results in the query context
        /// But discard them if the Storage has been modified
        /// In an ideal situation we would only discard the scalars affected by the storage change
        if (!options.only_analyze && context->hasQueryContext() && !context->getViewSource())
            for (const auto & it : syntax_analyzer_result->getScalars())
                context->getQueryContext()->addScalar(it.first, it.second);

        if (view)
        {
            /// Restore original view name. Save rewritten subquery for future usage in StorageView.
            query_info.view_query = view->restoreViewName(getSelectQuery(), view_table);
            view = nullptr;
        }

        if (try_move_to_prewhere && storage && storage->canMoveConditionsToPrewhere() && query.where() && !query.prewhere())
        {
            /// PREWHERE optimization: transfer some condition from WHERE to PREWHERE if enabled and viable
            if (const auto & column_sizes = storage->getColumnSizes(); !column_sizes.empty())
            {
                /// Extract column compressed sizes.
                std::unordered_map<std::string, UInt64> column_compressed_sizes;
                for (const auto & [name, sizes] : column_sizes)
                    column_compressed_sizes[name] = sizes.data_compressed;

                SelectQueryInfo current_info;
                current_info.query = query_ptr;
                current_info.syntax_analyzer_result = syntax_analyzer_result;

                MergeTreeWhereOptimizer{
                    current_info,
                    context,
                    std::move(column_compressed_sizes),
                    metadata_snapshot,
                    syntax_analyzer_result->requiredSourceColumns(),
                    log};
            }
        }

        if (query.prewhere() && query.where())
        {
            /// Filter block in WHERE instead to get better performance
            query.setExpression(
                ASTSelectQuery::Expression::WHERE, makeASTFunction("and", query.prewhere()->clone(), query.where()->clone()));
        }

        query_analyzer = std::make_unique<SelectQueryExpressionAnalyzer>(
            query_ptr,
            syntax_analyzer_result,
            context,
            metadata_snapshot,
            NameSet(required_result_column_names_p->begin(), required_result_column_names_p->end()),
            !options.only_analyze,
            options,
            prepared_sets);

        if (!options.only_analyze)
        {
            if (query.sampleSize() && (input_pipe || !storage || !storage->supportsSampling()))
                throw Exception("Illegal SAMPLE: stream doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

            if (query.final() && (input_pipe || !storage || !storage->supportsFinal()))
                throw Exception(
                    (!input_pipe && storage) ? "Storage " + storage->getName() + " doesn't support FINAL" : "Illegal FINAL",
                    ErrorCodes::ILLEGAL_FINAL);

            if (query.prewhere() && (input_pipe || !storage || !storage->supportsPrewhere()))
                throw Exception(
                    (!input_pipe && storage) ? "Storage " + storage->getName() + " doesn't support PREWHERE" : "Illegal PREWHERE",
                    ErrorCodes::ILLEGAL_PREWHERE);

            /// Save the new temporary tables in the query context
            for (const auto & it : query_analyzer->getExternalTables())
                if (!context->tryResolveStorageID({"", it.first}, Context::ResolveExternal))
                    context->addExternalTable(it.first, std::move(*it.second));
        }

        if (!options.only_analyze || options.modify_inplace)
        {
            if (syntax_analyzer_result->rewrite_subqueries)
            {
                /// remake interpreter_subquery when PredicateOptimizer rewrites subqueries and main table is subquery
                interpreter_subquery = joined_tables.makeLeftTableSubquery(options.subquery());
            }
        }

        if (interpreter_subquery)
        {
            /// If there is an aggregation in the outer query, WITH TOTALS is ignored in the subquery.
            if (query_analyzer->hasAggregation())
                interpreter_subquery->ignoreWithTotals();
        }

        required_columns = syntax_analyzer_result->requiredSourceColumns();
        if (storage)
        {
            /// Fix source_header for filter actions.
            if (row_policy_filter)
            {
                filter_info = std::make_shared<FilterDAGInfo>();
                filter_info->column_name = generateFilterActions(filter_info->actions, required_columns);

                auto required_columns_from_filter = filter_info->actions->getRequiredColumns();

                for (const auto & column : required_columns_from_filter)
                {
                    if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column.name))
                        required_columns.push_back(column.name);
                }
            }

            source_header = storage_snapshot->getSampleBlockForColumns(required_columns);
        }

        /// proton: starts.
        /// Analyze event predicates in WHERE clause like `WHERE _tp_time > 2023-01-01 00:01:01` or `WHERE _tp_sn > 1000`
        /// and create `SeekToInfo` objects to represent these predicates for streaming store rewinding in a streaming query.
        analyzeEventPredicateAsSeekTo(joined_tables);

        /// Calculate structure of the result.
        result_header = getSampleBlockImpl();
        /// proton, FIXME. For distributed streaming query in future, we may need conditionally remove __tp_ts from result header
        /// depending on the query stage

        /// for distributed historic query, remove 'window_start', 'window_end' column
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        {
            if (result_header.findByName(ProtonConsts::STREAMING_WINDOW_START))
                result_header.erase(ProtonConsts::STREAMING_WINDOW_START);

            if (result_header.findByName(ProtonConsts::STREAMING_WINDOW_END))
                result_header.erase(ProtonConsts::STREAMING_WINDOW_END);
        }

        if (result_header.findByName(ProtonConsts::STREAMING_TIMESTAMP_ALIAS))
            result_header.erase(ProtonConsts::STREAMING_TIMESTAMP_ALIAS);
        /// proton: ends.
    };

    /// proton: starts.
    /// Try to do some preparation for functions in streaming queries
    checkAndPrepareStreamingFunctions();
    /// proto: ends

    analyze(shouldMoveToPrewhere());

    bool need_analyze_again = false;
    if (analysis_result.prewhere_constant_filter_description.always_false || analysis_result.prewhere_constant_filter_description.always_true)
    {
        if (analysis_result.prewhere_constant_filter_description.always_true)
            query.setExpression(ASTSelectQuery::Expression::PREWHERE, {});
        else
            query.setExpression(ASTSelectQuery::Expression::PREWHERE, std::make_shared<ASTLiteral>(false));
        need_analyze_again = true;
    }

    if (analysis_result.where_constant_filter_description.always_false || analysis_result.where_constant_filter_description.always_true)
    {
        if (analysis_result.where_constant_filter_description.always_true)
            query.setExpression(ASTSelectQuery::Expression::WHERE, {});
        else
            query.setExpression(ASTSelectQuery::Expression::WHERE, std::make_shared<ASTLiteral>(false));
        need_analyze_again = true;
    }

    if (need_analyze_again)
    {
        LOG_TRACE(log, "Running 'analyze' second time");

        /// Reuse already built sets for multiple passes of analysis
        prepared_sets = query_analyzer->getPreparedSets();

        /// Do not try move conditions to PREWHERE for the second time.
        /// Otherwise, we won't be able to fallback from inefficient PREWHERE to WHERE later.
        analyze(/* try_move_to_prewhere = */ false);
    }

    /// If there is no WHERE, filter blocks as usual
    if (query.prewhere() && !query.where())
        analysis_result.prewhere_info->need_filter = true;

    if (table_id && got_storage_from_query && !joined_tables.isLeftTableFunction())
    {
        /// The current user should have the SELECT privilege. If this table_id is for a table
        /// function we don't check access rights here because in this case they have been already
        /// checked in ITableFunction::execute().
        checkAccessRightsForSelect(context, table_id, metadata_snapshot, *syntax_analyzer_result);

        /// Remove limits for some tables in the `system` database.
        if (shouldIgnoreQuotaAndLimits(table_id) && (joined_tables.tablesCount() <= 1))
        {
            options.ignore_quota = true;
            options.ignore_limits = true;
        }
    }

    /// Add prewhere actions with alias columns and record needed columns from storage.
    if (storage)
    {
        addPrewhereAliasActions();
        analysis_result.required_columns = required_columns;
    }

    /// proton: starts
    finalCheckAndOptimizeForStreamingQuery();

    if (query_info.projection)
        storage_snapshot->addProjection(query_info.projection->desc);

    /// Blocks used in expression analysis contains size 1 const columns for constant folding and
    ///  null non-const columns to avoid useless memory allocations. However, a valid block sample
    ///  requires all columns to be of size 0, thus we need to sanitize the block here.
    sanitizeBlock(result_header, true);
}

void InterpreterSelectQuery::buildQueryPlan(QueryPlan & query_plan)
{
    executeImpl(query_plan, std::move(input_pipe));

    /// We must guarantee that result structure is the same as in getSampleBlock()
    ///
    /// But if it's a projection query, plan header does not match result_header.
    /// TODO: add special stage for InterpreterSelectQuery?
    if (!options.is_projection_query && !blocksHaveEqualStructure(query_plan.getCurrentDataStream().header, result_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
            result_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);

        auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
        query_plan.addStep(std::move(converting));
    }

    /// Extend lifetime of context, table lock, storage.
    query_plan.addInterpreterContext(context);
    if (table_lock)
        query_plan.addTableLock(std::move(table_lock));
    if (storage)
        query_plan.addStorageHolder(storage);
}

BlockIO InterpreterSelectQuery::execute()
{
    BlockIO res;
    QueryPlan query_plan;

    buildQueryPlan(query_plan);

    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context), context);

    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    setQuota(res.pipeline);

    return res;
}

Block InterpreterSelectQuery::getSampleBlockImpl()
{
    auto & select_query = getSelectQuery();

    query_info.query = query_ptr;

    /// NOTE: this is required for getQueryProcessingStage(), so should be initialized before ExpressionAnalysisResult.
    query_info.has_window = query_analyzer->hasWindow();
    if (storage && !options.only_analyze)
    {
        query_analyzer->makeSetsForIndex(select_query.where());
        query_analyzer->makeSetsForIndex(select_query.prewhere());
        query_info.prepared_sets = query_analyzer->getPreparedSets();

        from_stage = storage->getQueryProcessingStage(context, options.to_stage, storage_snapshot, query_info);
    }

    /// Do I need to perform the first part of the pipeline?
    /// Running on remote servers during distributed processing or if query is not distributed.
    ///
    /// Also note that with distributed_group_by_no_merge=1 or when there is
    /// only one remote server, it is equal to local query in terms of query
    /// stages (or when due to optimize_distributed_group_by_sharding_key the query was processed up to Complete stage).
    bool first_stage = from_stage < QueryProcessingStage::WithMergeableState
        && options.to_stage >= QueryProcessingStage::WithMergeableState;
    /// Do I need to execute the second part of the pipeline?
    /// Running on the initiating server during distributed processing or if query is not distributed.
    ///
    /// Also note that with distributed_group_by_no_merge=2 (i.e. when optimize_distributed_group_by_sharding_key takes place)
    /// the query on the remote server will be processed up to WithMergeableStateAfterAggregationAndLimit,
    /// So it will do partial second stage (second_stage=true), and initiator will do the final part.
    bool second_stage = from_stage <= QueryProcessingStage::WithMergeableState
        && options.to_stage > QueryProcessingStage::WithMergeableState;

    analysis_result = ExpressionAnalysisResult(
        *query_analyzer,
        metadata_snapshot,
        first_stage,
        second_stage,
        options.only_analyze,
        filter_info,
        source_header,
        Streaming::ExpressionAnalysisContext{.emit_version = emit_version, .data_stream_semantic = getDataStreamSemantic()});

    if (options.to_stage == QueryProcessingStage::Enum::FetchColumns)
    {
        auto header = source_header;

        if (analysis_result.prewhere_info)
        {
            header = analysis_result.prewhere_info->prewhere_actions->updateHeader(header);
            if (analysis_result.prewhere_info->remove_prewhere_column)
                header.erase(analysis_result.prewhere_info->prewhere_column_name);
        }
        return header;
    }

    if (options.to_stage == QueryProcessingStage::Enum::WithMergeableState)
    {
        if (!analysis_result.need_aggregate)
        {
            // What's the difference with selected_columns?
            // Here we calculate the header we want from remote server after it
            // executes query up to WithMergeableState. When there is an ORDER BY,
            // it is executed on remote server firstly, then we execute merge
            // sort on initiator. To execute ORDER BY, we need to calculate the
            // ORDER BY keys. These keys might be not present among the final
            // SELECT columns given by the `selected_column`. This is why we have
            // to use proper keys given by the result columns of the
            // `before_order_by` expression actions.
            // Another complication is window functions -- if we have them, they
            // are calculated on initiator, before ORDER BY columns. In this case,
            // the shard has to return columns required for window function
            // calculation and further steps, given by the `before_window`
            // expression actions.
            // As of 21.6 this is broken: the actions in `before_window` might
            // not contain everything required for the ORDER BY step, but this
            // is a responsibility of ExpressionAnalyzer and is not a problem
            // with this code. See
            // https://github.com/ClickHouse/ClickHouse/issues/19857 for details.
            if (analysis_result.before_window)
                return analysis_result.before_window->getResultColumns();

            return analysis_result.before_order_by->getResultColumns();
        }

        Block header = analysis_result.before_aggregation->getResultColumns();

        Block res;

        if (analysis_result.use_grouping_set_key)
            res.insert({ nullptr, std::make_shared<DataTypeUInt64>(), "__grouping_set" });

        for (const auto & key : query_analyzer->aggregationKeys())
            res.insert({nullptr, header.getByName(key.name).type, key.name});

        for (const auto & aggregate : query_analyzer->aggregates())
        {
            size_t arguments_size = aggregate.argument_names.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = header.getByName(aggregate.argument_names[j]).type;

            DataTypePtr type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);

            res.insert({nullptr, type, aggregate.column_name});
        }

        return res;
    }

    if (options.to_stage >= QueryProcessingStage::Enum::WithMergeableStateAfterAggregation)
    {
        // It's different from selected_columns, see the comment above for
        // WithMergeableState stage.
        if (analysis_result.before_window)
            return analysis_result.before_window->getResultColumns();

        return analysis_result.before_order_by->getResultColumns();
    }

    return analysis_result.final_projection->getResultColumns();
}

static Field getWithFillFieldValue(const ASTPtr & node, ContextPtr context)
{
    auto [field, type] = evaluateConstantExpression(node, context);

    if (!isColumnedAsNumber(type))
        throw Exception("Illegal type " + type->getName() + " of WITH FILL expression, must be numeric type", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

    return field;
}

static std::pair<Field, std::optional<IntervalKind>> getWithFillStep(const ASTPtr & node, const ContextPtr & context)
{
    auto [field, type] = evaluateConstantExpression(node, context);

    if (const auto * type_interval = typeid_cast<const DataTypeInterval *>(type.get()))
        return std::make_pair(std::move(field), type_interval->getKind());

    if (isColumnedAsNumber(type))
        return std::make_pair(std::move(field), std::nullopt);

    throw Exception("Illegal type " + type->getName() + " of WITH FILL expression, must be numeric type", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
}

static FillColumnDescription getWithFillDescription(const ASTOrderByElement & order_by_elem, const ContextPtr & context)
{
    FillColumnDescription descr;

    if (order_by_elem.fill_from)
        descr.fill_from = getWithFillFieldValue(order_by_elem.fill_from, context);
    if (order_by_elem.fill_to)
        descr.fill_to = getWithFillFieldValue(order_by_elem.fill_to, context);

    if (order_by_elem.fill_step)
        std::tie(descr.fill_step, descr.step_kind) = getWithFillStep(order_by_elem.fill_step, context);
    else
        descr.fill_step = order_by_elem.direction;

    if (applyVisitor(FieldVisitorAccurateEquals(), descr.fill_step, Field{0}))
        throw Exception("WITH FILL STEP value cannot be zero", ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

    if (order_by_elem.direction == 1)
    {
        if (applyVisitor(FieldVisitorAccurateLess(), descr.fill_step, Field{0}))
            throw Exception("WITH FILL STEP value cannot be negative for sorting in ascending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

        if (!descr.fill_from.isNull() && !descr.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), descr.fill_to, descr.fill_from))
        {
            throw Exception("WITH FILL TO value cannot be less than FROM value for sorting in ascending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
        }
    }
    else
    {
        if (applyVisitor(FieldVisitorAccurateLess(), Field{0}, descr.fill_step))
            throw Exception("WITH FILL STEP value cannot be positive for sorting in descending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);

        if (!descr.fill_from.isNull() && !descr.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), descr.fill_from, descr.fill_to))
        {
            throw Exception("WITH FILL FROM value cannot be less than TO value for sorting in descending direction",
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION);
        }
    }

    return descr;
}

static SortDescription getSortDescription(const ASTSelectQuery & query, const ContextPtr & context_)
{
    SortDescription order_descr;
    order_descr.reserve(query.orderBy()->children.size());

    for (const auto & elem : query.orderBy()->children)
    {
        auto column_name = elem->children.front()->getColumnName();
        const auto & order_by_elem = elem->as<ASTOrderByElement &>();

        std::shared_ptr<Collator> collator;
        if (order_by_elem.collation)
            collator = std::make_shared<Collator>(order_by_elem.collation->as<ASTLiteral &>().value.get<String>());

        if (order_by_elem.with_fill)
        {
            FillColumnDescription fill_desc = getWithFillDescription(order_by_elem, context_);
            order_descr.emplace_back(std::move(column_name), order_by_elem.direction, order_by_elem.nulls_direction, collator, true, fill_desc);
        }
        else
            order_descr.emplace_back(std::move(column_name), order_by_elem.direction, order_by_elem.nulls_direction, collator);
    }

    return order_descr;
}

static SortDescription getSortDescriptionFromGroupBy(const ASTSelectQuery & query)
{
    SortDescription order_descr;
    order_descr.reserve(query.groupBy()->children.size());

    for (const auto & elem : query.groupBy()->children)
        order_descr.emplace_back(elem->getColumnName(), 1, 1);

    return order_descr;
}

static UInt64 getLimitUIntValue(const ASTPtr & node, const ContextPtr & context, const std::string & expr)
{
    const auto & [field, type] = evaluateConstantExpression(node, context);

    if (!isNativeNumber(type))
        throw Exception(
            "Illegal type " + type->getName() + " of " + expr + " expression, must be numeric type", ErrorCodes::INVALID_LIMIT_EXPRESSION);

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(
            "The value " + applyVisitor(FieldVisitorToString(), field) + " of " + expr + " expression is not representable as uint64",
            ErrorCodes::INVALID_LIMIT_EXPRESSION);

    return converted.safeGet<UInt64>();
}


static std::pair<UInt64, UInt64> getLimitLengthAndOffset(const ASTSelectQuery & query, const ContextPtr & context)
{
    UInt64 length = 0;
    UInt64 offset = 0;

    if (query.limitLength())
    {
        length = getLimitUIntValue(query.limitLength(), context, "LIMIT");
        if (query.limitOffset() && length)
            offset = getLimitUIntValue(query.limitOffset(), context, "OFFSET");
    }
    else if (query.limitOffset())
        offset = getLimitUIntValue(query.limitOffset(), context, "OFFSET");
    return {length, offset};
}


static UInt64 getLimitForSorting(const ASTSelectQuery & query, const ContextPtr & context_)
{
    /// Partial sort can be done if there is LIMIT but no DISTINCT or LIMIT BY, neither ARRAY JOIN.
    if (!query.distinct && !query.limitBy() && !query.limit_with_ties && !query.arrayJoinExpressionList().first && query.limitLength())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context_);
        if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
            return 0;

        return limit_length + limit_offset;
    }
    return 0;
}


static bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery & query)
{
    if (query.group_by_with_totals)
        return true;

    /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
     * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
     */
    if (auto query_table = extractTableExpression(query, 0))
    {
        if (const auto * ast_union = query_table->as<ASTSelectWithUnionQuery>())
        {
            /** NOTE
            * 1. For ASTSelectWithUnionQuery after normalization for union child node the height of the AST tree is at most 2.
            * 2. For ASTSelectIntersectExceptQuery after normalization in case there are intersect or except nodes,
            * the height of the AST tree can have any depth (each intersect/except adds a level), but the
            * number of children in those nodes is always 2.
            */
            std::function<bool(ASTPtr)> traverse_recursively = [&](ASTPtr child_ast) -> bool
            {
                if (const auto * select_child = child_ast->as <ASTSelectQuery>())
                {
                    if (hasWithTotalsInAnySubqueryInFromClause(select_child->as<ASTSelectQuery &>()))
                        return true;
                }
                else if (const auto * union_child = child_ast->as<ASTSelectWithUnionQuery>())
                {
                    for (const auto & subchild : union_child->list_of_selects->children)
                        if (traverse_recursively(subchild))
                            return true;
                }
                else if (const auto * intersect_child = child_ast->as<ASTSelectIntersectExceptQuery>())
                {
                    auto selects = intersect_child->getListOfSelects();
                    for (const auto & subchild : selects)
                        if (traverse_recursively(subchild))
                            return true;
                }
                return false;
            };

            for (const auto & elem : ast_union->list_of_selects->children)
                if (traverse_recursively(elem))
                    return true;
        }
    }

    return false;
}


void InterpreterSelectQuery::executeImpl(QueryPlan & query_plan, std::optional<Pipe> prepared_pipe)
{
    /** Streams of data. When the query is executed in parallel, we have several data streams.
     *  If there is no GROUP BY, then perform all operations before ORDER BY and LIMIT in parallel, then
     *  if there is an ORDER BY, then glue the streams using ResizeProcessor, and then MergeSorting transforms,
     *  if not, then glue it using ResizeProcessor,
     *  then apply LIMIT.
     *  If there is GROUP BY, then we will perform all operations up to GROUP BY, inclusive, in parallel;
     *  a parallel GROUP BY will glue streams into one,
     *  then perform the remaining operations with one resulting stream.
     */

    /// Now we will compose block streams that perform the necessary actions.
    auto & query = getSelectQuery();
    const Settings & settings = context->getSettingsRef();
    auto & expressions = analysis_result;
    bool intermediate_stage = false;
    bool to_aggregation_stage = false;
    bool from_aggregation_stage = false;

    /// Do I need to aggregate in a separate row that has not passed max_rows_to_group_by?
    bool aggregate_overflow_row =
        expressions.need_aggregate &&
        query.group_by_with_totals &&
        settings.max_rows_to_group_by &&
        settings.group_by_overflow_mode == OverflowMode::ANY &&
        settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

    /// Do I need to immediately finalize the aggregate functions after the aggregation?
    bool aggregate_final =
        expressions.need_aggregate &&
        options.to_stage > QueryProcessingStage::WithMergeableState &&
        !query.group_by_with_totals && !query.group_by_with_rollup && !query.group_by_with_cube;

    bool use_grouping_set_key = expressions.use_grouping_set_key;

    if (query.group_by_with_grouping_sets && query.group_by_with_totals)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and GROUPING SETS are not supported together");

    if (query.group_by_with_grouping_sets && (query.group_by_with_rollup || query.group_by_with_cube))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GROUPING SETS are not supported together with ROLLUP and CUBE");

    if (expressions.hasHaving() && query.group_by_with_totals && (query.group_by_with_rollup || query.group_by_with_cube))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING");

    if (query_info.projection && query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
    {
        query_info.projection->aggregate_overflow_row = aggregate_overflow_row;
        query_info.projection->aggregate_final = aggregate_final;
    }

    if (options.only_analyze)
    {
        /// proton: starts. Propogate streaming flag to NullSource
        auto read_nothing = std::make_unique<ReadNothingStep>(source_header, isStreamingQuery());
        /// proton: ends.
        query_plan.addStep(std::move(read_nothing));

        if (expressions.filter_info)
        {
            auto row_level_security_step = std::make_unique<FilterStep>(
                query_plan.getCurrentDataStream(),
                expressions.filter_info->actions,
                expressions.filter_info->column_name,
                expressions.filter_info->do_remove_column);

            row_level_security_step->setStepDescription("Row-level security filter");
            query_plan.addStep(std::move(row_level_security_step));
        }

        if (expressions.prewhere_info)
        {
            if (expressions.prewhere_info->row_level_filter)
            {
                auto row_level_filter_step = std::make_unique<FilterStep>(
                    query_plan.getCurrentDataStream(),
                    expressions.prewhere_info->row_level_filter,
                    expressions.prewhere_info->row_level_column_name,
                    true);

                row_level_filter_step->setStepDescription("Row-level security filter (PREWHERE)");
                query_plan.addStep(std::move(row_level_filter_step));
            }

            auto prewhere_step = std::make_unique<FilterStep>(
                query_plan.getCurrentDataStream(),
                expressions.prewhere_info->prewhere_actions,
                expressions.prewhere_info->prewhere_column_name,
                expressions.prewhere_info->remove_prewhere_column);

            prewhere_step->setStepDescription("PREWHERE");
            query_plan.addStep(std::move(prewhere_step));
        }
    }
    else
    {
        if (prepared_pipe)
        {
            auto prepared_source_step = std::make_unique<ReadFromPreparedSource>(std::move(*prepared_pipe));
            query_plan.addStep(std::move(prepared_source_step));
            query_plan.addInterpreterContext(context);
        }

        if (from_stage == QueryProcessingStage::WithMergeableState &&
            options.to_stage == QueryProcessingStage::WithMergeableState)
            intermediate_stage = true;

        /// Support optimize_distributed_group_by_sharding_key
        /// Is running on the initiating server during distributed processing?
        if (from_stage >= QueryProcessingStage::WithMergeableStateAfterAggregation)
            from_aggregation_stage = true;
        /// Is running on remote servers during distributed processing?
        if (options.to_stage >= QueryProcessingStage::WithMergeableStateAfterAggregation)
            to_aggregation_stage = true;

        /// Read the data from Storage. from_stage - to what stage the request was completed in Storage.
        executeFetchColumns(from_stage, query_plan);

        LOG_TRACE(log, "{} -> {}", QueryProcessingStage::toString(from_stage), QueryProcessingStage::toString(options.to_stage));
    }

    if (options.to_stage > QueryProcessingStage::FetchColumns)
    {
        auto preliminary_sort = [&]()
        {
            /** For distributed query processing,
              *  if no GROUP, HAVING set,
              *  but there is an ORDER or LIMIT,
              *  then we will perform the preliminary sorting and LIMIT on the remote server.
              */
            if (!expressions.second_stage
                && !expressions.need_aggregate
                && !expressions.hasHaving()
                && !expressions.has_window)
            {
                if (expressions.has_order_by)
                    executeOrder(
                        query_plan,
                        query_info.input_order_info ? query_info.input_order_info
                                                    : (query_info.projection ? query_info.projection->input_order_info : nullptr));

                if (expressions.has_order_by && query.limitLength())
                    executeDistinct(query_plan, false, expressions.selected_columns, true);

                if (expressions.hasLimitBy())
                {
                    executeExpression(query_plan, expressions.before_limit_by, "Before LIMIT BY");
                    executeLimitBy(query_plan);
                }

                if (query.limitLength())
                    executePreLimit(query_plan, true);
            }
        };

        if (intermediate_stage)
        {
            if (expressions.first_stage || expressions.second_stage)
                throw Exception("Query with intermediate stage cannot have any other stages", ErrorCodes::LOGICAL_ERROR);

            preliminary_sort();
            if (expressions.need_aggregate)
                executeMergeAggregated(query_plan, aggregate_overflow_row, aggregate_final, use_grouping_set_key);
        }

        if (from_aggregation_stage)
        {
            if (intermediate_stage || expressions.first_stage || expressions.second_stage)
                throw Exception("Query with after aggregation stage cannot have any other stages", ErrorCodes::LOGICAL_ERROR);
        }

        if (expressions.first_stage)
        {
            /// proton : starts. If we only fetch columns, don't need add streaming processing step, e.g. `WatermarkStep` etc.
            buildStreamingProcessingQueryPlanBeforeJoin(query_plan);
            /// proton: ends

            // If there is a storage that supports prewhere, this will always be nullptr
            // Thus, we don't actually need to check if projection is active.
            if (!query_info.projection && expressions.filter_info)
            {
                auto row_level_security_step = std::make_unique<FilterStep>(
                    query_plan.getCurrentDataStream(),
                    expressions.filter_info->actions,
                    expressions.filter_info->column_name,
                    expressions.filter_info->do_remove_column);

                row_level_security_step->setStepDescription("Row-level security filter");
                query_plan.addStep(std::move(row_level_security_step));
            }

            if (expressions.before_array_join)
            {
                QueryPlanStepPtr before_array_join_step
                    = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), expressions.before_array_join);
                before_array_join_step->setStepDescription("Before ARRAY JOIN");
                query_plan.addStep(std::move(before_array_join_step));
            }

            if (expressions.array_join)
            {
                QueryPlanStepPtr array_join_step
                    = std::make_unique<ArrayJoinStep>(query_plan.getCurrentDataStream(), expressions.array_join);

                array_join_step->setStepDescription("ARRAY JOIN");
                query_plan.addStep(std::move(array_join_step));
            }

            if (expressions.before_join)
            {
                QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(
                    query_plan.getCurrentDataStream(),
                    expressions.before_join);
                before_join_step->setStepDescription("Before JOIN");
                query_plan.addStep(std::move(before_join_step));
            }

            /// Optional step to convert key columns to common supertype.
            if (expressions.converting_join_columns)
            {
                QueryPlanStepPtr convert_join_step = std::make_unique<ExpressionStep>(
                    query_plan.getCurrentDataStream(),
                    expressions.converting_join_columns);
                convert_join_step->setStepDescription("Convert JOIN columns");
                query_plan.addStep(std::move(convert_join_step));
            }

            if (expressions.hasJoin())
            {
                if (expressions.join->isFilled())
                {
                    QueryPlanStepPtr filled_join_step = std::make_unique<FilledJoinStep>(
                        query_plan.getCurrentDataStream(),
                        expressions.join,
                        settings.max_block_size);

                    filled_join_step->setStepDescription("JOIN");
                    query_plan.addStep(std::move(filled_join_step));
                }
                else
                {
                    auto joined_plan = query_analyzer->getJoinedPlan();

                    if (!joined_plan)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no joined plan for query");

                    auto add_sorting = [&settings, this] (QueryPlan & plan, const Names & key_names)
                    {
                        SortDescription order_descr;
                        order_descr.reserve(key_names.size());
                        for (const auto & key_name : key_names)
                            order_descr.emplace_back(key_name);

                        auto sorting_step = std::make_unique<SortingStep>(
                            plan.getCurrentDataStream(),
                            std::move(order_descr),
                            settings.max_block_size,
                            0 /* LIMIT */,
                            SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
                            settings.max_bytes_before_remerge_sort,
                            settings.remerge_sort_lowered_memory_bytes_ratio,
                            settings.max_bytes_before_external_sort,
                            this->context->getTemporaryVolume(),
                            settings.min_free_disk_space_for_temporary_data,
                            settings.optimize_sorting_by_input_stream_properties);
                        sorting_step->setStepDescription("Sort before JOIN");
                        plan.addStep(std::move(sorting_step));
                    };

                    if (expressions.join->pipelineType() == JoinPipelineType::YShaped)
                    {
                        const auto & join_clause = expressions.join->getTableJoin().getOnlyClause();
                        add_sorting(query_plan, join_clause.key_names_left);
                        add_sorting(*joined_plan, join_clause.key_names_right);
                    }

                    /// proton : starts
                    QueryPlanStepPtr join_step;
                    if (joined_plan->isStreaming())
                    {
                        join_step = std::make_unique<Streaming::JoinStep>(
                            query_plan.getCurrentDataStream(),
                            joined_plan->getCurrentDataStream(),
                            expressions.join,
                            settings.max_block_size,
                            max_streams,
                            settings.join_max_buffered_bytes);
                    }
                    else
                    {
                        join_step = std::make_unique<JoinStep>(
                            query_plan.getCurrentDataStream(),
                            joined_plan->getCurrentDataStream(),
                            expressions.join,
                            settings.max_block_size,
                            max_streams,
                            analysis_result.optimize_read_in_order);
                    }
                    /// proton : ends

                    join_step->setStepDescription(fmt::format("JOIN {}", expressions.join->pipelineType()));
                    std::vector<QueryPlanPtr> plans;
                    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
                    plans.emplace_back(std::move(joined_plan));

                    query_plan = QueryPlan();
                    query_plan.unitePlans(std::move(join_step), {std::move(plans)});
                }
            }

            /// proton: starts. Build some streaming processing steps after joined multiples streams
            buildStreamingProcessingQueryPlanAfterJoin(query_plan);
            /// proton: ends.

            if (!query_info.projection && expressions.hasWhere())
                executeWhere(query_plan, expressions.before_where, expressions.remove_where_filter);

            /// proton : starts. TODO, when we support arbitrary shuffle expr, moved to ExpressionAnalyzer
            /// TODO, if there is no aggregation / or parent select doesn't have aggregation (recursively)
            /// avoid shuffle by step as an optimization
            if (query.shuffleBy())
                executeLightShuffling(query_plan);
            /// proton : ends

            if (expressions.need_aggregate)
            {
                executeAggregation(
                    query_plan, expressions.before_aggregation, aggregate_overflow_row, aggregate_final, query_info.input_order_info);
                /// We need to reset input order info, so that executeOrder can't use  it
                query_info.input_order_info.reset();
            }

            // Now we must execute:
            // 1) expressions before window functions,
            // 2) window functions,
            // 3) expressions after window functions,
            // 4) preliminary distinct.
            // This code decides which part we execute on shard (first_stage)
            // and which part on initiator (second_stage). See also the counterpart
            // code for "second_stage" that has to execute the rest.
            if (expressions.need_aggregate)
            {
                // We have aggregation, so we can't execute any later-stage
                // expressions on shards, neither "before window functions" nor
                // "before ORDER BY".
            }
            else
            {
                // We don't have aggregation.
                // Window functions must be executed on initiator (second_stage).
                // ORDER BY and DISTINCT might depend on them, so if we have
                // window functions, we can't execute ORDER BY and DISTINCT
                // now, on shard (first_stage).
                if (query_analyzer->hasWindow())
                {
                    executeExpression(query_plan, expressions.before_window, "Before window functions");
                }
                else
                {
                    // We don't have window functions, so we can execute the
                    // expressions before ORDER BY and the preliminary DISTINCT
                    // now, on shards (first_stage).
                    assert(!expressions.before_window);
                    executeExpression(query_plan, expressions.before_order_by, "Before ORDER BY");
                    executeDistinct(query_plan, true, expressions.selected_columns, true);
                }
            }

            preliminary_sort();
        }

        if (expressions.second_stage || from_aggregation_stage)
        {
            if (from_aggregation_stage)
            {
                /// No need to aggregate anything, since this was done on remote shards.
            }
            else if (expressions.need_aggregate)
            {
                /// If you need to combine aggregated results from multiple servers
                if (!expressions.first_stage)
                    executeMergeAggregated(query_plan, aggregate_overflow_row, aggregate_final, use_grouping_set_key);

                if (!aggregate_final)
                {
                    if (query.group_by_with_totals)
                    {
                        bool final = !query.group_by_with_rollup && !query.group_by_with_cube;
                        executeTotalsAndHaving(
                            query_plan, expressions.hasHaving(), expressions.before_having, expressions.remove_having_filter, aggregate_overflow_row, final);
                    }

                    if (query.group_by_with_rollup)
                        executeRollupOrCube(query_plan, Modificator::ROLLUP);
                    else if (query.group_by_with_cube)
                        executeRollupOrCube(query_plan, Modificator::CUBE);

                    if ((query.group_by_with_rollup || query.group_by_with_cube || query.group_by_with_grouping_sets) && expressions.hasHaving())
                        executeHaving(query_plan, expressions.before_having, expressions.remove_having_filter);
                }
                else if (expressions.hasHaving())
                    executeHaving(query_plan, expressions.before_having, expressions.remove_having_filter);
            }
            else if (query.group_by_with_totals || query.group_by_with_rollup || query.group_by_with_cube || query.group_by_with_grouping_sets)
                throw Exception("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS are not supported without aggregation", ErrorCodes::NOT_IMPLEMENTED);

            // Now we must execute:
            // 1) expressions before window functions,
            // 2) window functions,
            // 3) expressions after window functions,
            // 4) preliminary distinct.
            // Some of these were already executed at the shards (first_stage),
            // see the counterpart code and comments there.
            if (from_aggregation_stage)
            {
                if (query_analyzer->hasWindow())
                    throw Exception(
                        "Window functions does not support processing from WithMergeableStateAfterAggregation",
                        ErrorCodes::NOT_IMPLEMENTED);
            }
            else if (expressions.need_aggregate)
            {
                executeExpression(query_plan, expressions.before_window,
                    "Before window functions");
                executeWindow(query_plan);
                executeExpression(query_plan, expressions.before_order_by, "Before ORDER BY");
                executeDistinct(query_plan, true, expressions.selected_columns, true);
            }
            else
            {
                if (query_analyzer->hasWindow())
                {
                    executeWindow(query_plan);
                    executeExpression(query_plan, expressions.before_order_by, "Before ORDER BY");
                    executeDistinct(query_plan, true, expressions.selected_columns, true);
                }
                else
                {
                    // Neither aggregation nor windows, all expressions before
                    // ORDER BY executed on shards.
                }
            }

            if (expressions.has_order_by)
            {
                /** If there is an ORDER BY for distributed query processing,
                  *  but there is no aggregation, then on the remote servers ORDER BY was made
                  *  - therefore, we merge the sorted streams from remote servers.
                  *
                  * Also in case of remote servers was process the query up to WithMergeableStateAfterAggregationAndLimit
                  * (distributed_group_by_no_merge=2 or optimize_distributed_group_by_sharding_key=1 takes place),
                  * then merge the sorted streams is enough, since remote servers already did full ORDER BY.
                  */

                if (from_aggregation_stage)
                    executeMergeSorted(query_plan, "after aggregation stage for ORDER BY");
                else if (!expressions.first_stage
                    && !expressions.need_aggregate
                    && !expressions.has_window
                    && !(query.group_by_with_totals && !aggregate_final))
                    executeMergeSorted(query_plan, "for ORDER BY, without aggregation");
                else    /// Otherwise, just sort.
                    executeOrder(
                        query_plan,
                        query_info.input_order_info ? query_info.input_order_info
                                                    : (query_info.projection ? query_info.projection->input_order_info : nullptr));
            }

            /** Optimization - if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
              * limiting the number of rows in each up to `offset + limit`.
              */
            bool has_withfill = false;
            if (query.orderBy())
            {
                SortDescription order_descr = getSortDescription(query, context);
                for (auto & desc : order_descr)
                    if (desc.with_fill)
                    {
                        has_withfill = true;
                        break;
                    }
            }

            bool apply_limit = options.to_stage != QueryProcessingStage::WithMergeableStateAfterAggregation;
            bool apply_prelimit = apply_limit &&
                                  query.limitLength() && !query.limit_with_ties &&
                                  !hasWithTotalsInAnySubqueryInFromClause(query) &&
                                  !query.arrayJoinExpressionList().first &&
                                  !query.distinct &&
                                  !expressions.hasLimitBy() &&
                                  !settings.extremes &&
                                  !has_withfill;
            bool apply_offset = options.to_stage != QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
            if (apply_prelimit)
            {
                executePreLimit(query_plan, /* do_not_skip_offset= */!apply_offset);
            }

            /** If there was more than one stream,
              * then DISTINCT needs to be performed once again after merging all streams.
              */
            if (!from_aggregation_stage && query.distinct)
                executeDistinct(query_plan, false, expressions.selected_columns, false);

            if (!from_aggregation_stage && expressions.hasLimitBy())
            {
                executeExpression(query_plan, expressions.before_limit_by, "Before LIMIT BY");
                executeLimitBy(query_plan);
            }

            executeWithFill(query_plan);

            /// If we have 'WITH TIES', we need execute limit before projection,
            /// because in that case columns from 'ORDER BY' are used.
            if (query.limit_with_ties && apply_offset)
            {
                executeLimit(query_plan);
            }

            /// Projection not be done on the shards, since then initiator will not find column in blocks.
            /// (significant only for WithMergeableStateAfterAggregation/WithMergeableStateAfterAggregationAndLimit).
            if (!to_aggregation_stage)
            {
                /// We must do projection after DISTINCT because projection may remove some columns.
                executeProjection(query_plan, expressions.final_projection);
            }

            /// Extremes are calculated before LIMIT, but after LIMIT BY. This is Ok.
            executeExtremes(query_plan);

            bool limit_applied = apply_prelimit || (query.limit_with_ties && apply_offset);
            /// Limit is no longer needed if there is prelimit.
            ///
            /// NOTE: that LIMIT cannot be applied if OFFSET should not be applied,
            /// since LIMIT will apply OFFSET too.
            /// This is the case for various optimizations for distributed queries,
            /// and when LIMIT cannot be applied it will be applied on the initiator anyway.
            if (apply_limit && !limit_applied && apply_offset)
                executeLimit(query_plan);

            if (apply_offset)
                executeOffset(query_plan);
        }
    }

    executeSubqueriesInSetsAndJoins(query_plan);
}

static void executeMergeAggregatedImpl(
    QueryPlan & query_plan,
    bool overflow_row,
    bool final,
    bool is_remote_storage,
    bool has_grouping_sets,
    const Settings & settings,
    const NamesAndTypesList & aggregation_keys,
    const AggregateDescriptions & aggregates)
{
    const auto & header_before_merge = query_plan.getCurrentDataStream().header;

    ColumnNumbers keys;
    if (has_grouping_sets)
        keys.push_back(header_before_merge.getPositionByName("__grouping_set"));
    for (const auto & key : aggregation_keys)
        keys.push_back(header_before_merge.getPositionByName(key.name));

    /** There are two modes of distributed aggregation.
      *
      * 1. In different threads read from the remote servers blocks.
      * Save all the blocks in the RAM. Merge blocks.
      * If the aggregation is two-level - parallelize to the number of buckets.
      *
      * 2. In one thread, read blocks from different servers in order.
      * RAM stores only one block from each server.
      * If the aggregation is a two-level aggregation, we consistently merge the blocks of each next level.
      *
      * The second option consumes less memory (up to 256 times less)
      *  in the case of two-level aggregation, which is used for large results after GROUP BY,
      *  but it can work more slowly.
      */

    Aggregator::Params params(header_before_merge, keys, aggregates, overflow_row, settings.max_threads);

    auto transform_params = std::make_shared<AggregatingTransformParams>(params, final, false);

    auto merging_aggregated = std::make_unique<MergingAggregatedStep>(
        query_plan.getCurrentDataStream(),
        std::move(transform_params),
        settings.distributed_aggregation_memory_efficient && is_remote_storage,
        settings.max_threads,
        settings.aggregation_memory_efficient_merge_threads);

    query_plan.addStep(std::move(merging_aggregated));
}

void InterpreterSelectQuery::addEmptySourceToQueryPlan(
    QueryPlan & query_plan, const Block & source_header, const SelectQueryInfo & query_info, const ContextPtr & context_)
{
    Pipe pipe(std::make_shared<NullSource>(source_header));

    PrewhereInfoPtr prewhere_info_ptr = query_info.projection ? query_info.projection->prewhere_info : query_info.prewhere_info;
    if (prewhere_info_ptr)
    {
        auto & prewhere_info = *prewhere_info_ptr;

        if (prewhere_info.row_level_filter)
        {
            pipe.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(header,
                    std::make_shared<ExpressionActions>(prewhere_info.row_level_filter),
                    prewhere_info.row_level_column_name, true);
            });
        }

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header, std::make_shared<ExpressionActions>(prewhere_info.prewhere_actions),
                prewhere_info.prewhere_column_name, prewhere_info.remove_prewhere_column);
        });
    }

    auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
    read_from_pipe->setStepDescription("Read from NullSource");
    query_plan.addStep(std::move(read_from_pipe));

    if (query_info.projection)
    {
        if (query_info.projection->before_where)
        {
            auto where_step = std::make_unique<FilterStep>(
                query_plan.getCurrentDataStream(),
                query_info.projection->before_where,
                query_info.projection->where_column_name,
                query_info.projection->remove_where_filter);

            where_step->setStepDescription("WHERE");
            query_plan.addStep(std::move(where_step));
        }

        if (query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
        {
            if (query_info.projection->before_aggregation)
            {
                auto expression_before_aggregation
                    = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), query_info.projection->before_aggregation);
                expression_before_aggregation->setStepDescription("Before GROUP BY");
                query_plan.addStep(std::move(expression_before_aggregation));
            }

            executeMergeAggregatedImpl(
                query_plan,
                query_info.projection->aggregate_overflow_row,
                query_info.projection->aggregate_final,
                false,
                false,
                context_->getSettingsRef(),
                query_info.projection->aggregation_keys,
                query_info.projection->aggregate_descriptions);
        }
    }
}

bool InterpreterSelectQuery::shouldMoveToPrewhere()
{
    const Settings & settings = context->getSettingsRef();
    const ASTSelectQuery & query = getSelectQuery();
    return settings.optimize_move_to_prewhere && (!query.final() || settings.optimize_move_to_prewhere_if_final);
}

void InterpreterSelectQuery::addPrewhereAliasActions()
{
    auto & expressions = analysis_result;
    if (expressions.filter_info)
    {
        if (!expressions.prewhere_info)
        {
            const bool does_storage_support_prewhere = !input_pipe && storage && storage->supportsPrewhere();
            if (does_storage_support_prewhere && shouldMoveToPrewhere())
            {
                /// Execute row level filter in prewhere as a part of "move to prewhere" optimization.
                expressions.prewhere_info = std::make_shared<PrewhereInfo>(
                    std::move(expressions.filter_info->actions),
                    std::move(expressions.filter_info->column_name));
                expressions.prewhere_info->prewhere_actions->projectInput(false);
                expressions.prewhere_info->remove_prewhere_column = expressions.filter_info->do_remove_column;
                expressions.prewhere_info->need_filter = true;
                expressions.filter_info = nullptr;
            }
        }
        else
        {
            /// Add row level security actions to prewhere.
            expressions.prewhere_info->row_level_filter = std::move(expressions.filter_info->actions);
            expressions.prewhere_info->row_level_column_name = std::move(expressions.filter_info->column_name);
            expressions.prewhere_info->row_level_filter->projectInput(false);
            expressions.filter_info = nullptr;
        }
    }

    auto & prewhere_info = analysis_result.prewhere_info;
    auto & columns_to_remove_after_prewhere = analysis_result.columns_to_remove_after_prewhere;

    /// Detect, if ALIAS columns are required for query execution
    auto alias_columns_required = false;
    const ColumnsDescription & storage_columns = metadata_snapshot->getColumns();
    for (const auto & column_name : required_columns)
    {
        auto column_default = storage_columns.getDefault(column_name);
        if (column_default && column_default->kind == ColumnDefaultKind::Alias)
        {
            alias_columns_required = true;
            break;
        }
    }

    /// There are multiple sources of required columns:
    ///  - raw required columns,
    ///  - columns deduced from ALIAS columns,
    ///  - raw required columns from PREWHERE,
    ///  - columns deduced from ALIAS columns from PREWHERE.
    /// PREWHERE is a special case, since we need to resolve it and pass directly to `IStorage::read()`
    /// before any other executions.
    if (alias_columns_required)
    {
        NameSet required_columns_from_prewhere; /// Set of all (including ALIAS) required columns for PREWHERE
        NameSet required_aliases_from_prewhere; /// Set of ALIAS required columns for PREWHERE

        if (prewhere_info)
        {
            /// Get some columns directly from PREWHERE expression actions
            auto prewhere_required_columns = prewhere_info->prewhere_actions->getRequiredColumns().getNames();
            required_columns_from_prewhere.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());

            if (prewhere_info->row_level_filter)
            {
                auto row_level_required_columns = prewhere_info->row_level_filter->getRequiredColumns().getNames();
                required_columns_from_prewhere.insert(row_level_required_columns.begin(), row_level_required_columns.end());
            }
        }

        /// Expression, that contains all raw required columns
        ASTPtr required_columns_all_expr = std::make_shared<ASTExpressionList>();

        /// Expression, that contains raw required columns for PREWHERE
        ASTPtr required_columns_from_prewhere_expr = std::make_shared<ASTExpressionList>();

        /// Sort out already known required columns between expressions,
        /// also populate `required_aliases_from_prewhere`.
        for (const auto & column : required_columns)
        {
            ASTPtr column_expr;
            const auto column_default = storage_columns.getDefault(column);
            bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;
            if (is_alias)
            {
                auto column_decl = storage_columns.get(column);
                column_expr = column_default->expression->clone();
                // recursive visit for alias to alias
                replaceAliasColumnsInQuery(
                    column_expr, metadata_snapshot->getColumns(), syntax_analyzer_result->array_join_result_to_source, context);

                column_expr = addTypeConversionToAST(
                    std::move(column_expr), column_decl.type->getName(), metadata_snapshot->getColumns().getAll(), context);
                column_expr = setAlias(column_expr, column);
            }
            else
                column_expr = std::make_shared<ASTIdentifier>(column);

            if (required_columns_from_prewhere.contains(column))
            {
                required_columns_from_prewhere_expr->children.emplace_back(std::move(column_expr));

                if (is_alias)
                    required_aliases_from_prewhere.insert(column);
            }
            else
                required_columns_all_expr->children.emplace_back(std::move(column_expr));
        }

        /// Columns, which we will get after prewhere and filter executions.
        NamesAndTypesList required_columns_after_prewhere;
        NameSet required_columns_after_prewhere_set;

        /// Collect required columns from prewhere expression actions.
        if (prewhere_info)
        {
            NameSet columns_to_remove(columns_to_remove_after_prewhere.begin(), columns_to_remove_after_prewhere.end());
            Block prewhere_actions_result = prewhere_info->prewhere_actions->getResultColumns();

            /// Populate required columns with the columns, added by PREWHERE actions and not removed afterwards.
            /// XXX: looks hacky that we already know which columns after PREWHERE we won't need for sure.
            for (const auto & column : prewhere_actions_result)
            {
                if (prewhere_info->remove_prewhere_column && column.name == prewhere_info->prewhere_column_name)
                    continue;

                if (columns_to_remove.contains(column.name))
                    continue;

                required_columns_all_expr->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                required_columns_after_prewhere.emplace_back(column.name, column.type);
            }

            required_columns_after_prewhere_set
                = collections::map<NameSet>(required_columns_after_prewhere, [](const auto & it) { return it.name; });
        }

        auto syntax_result
            = TreeRewriter(context).analyze(required_columns_all_expr, required_columns_after_prewhere, storage, storage_snapshot);
        alias_actions = ExpressionAnalyzer(required_columns_all_expr, syntax_result, context).getActionsDAG(true);

        /// The set of required columns could be added as a result of adding an action to calculate ALIAS.
        required_columns = alias_actions->getRequiredColumns().getNames();

        /// Do not remove prewhere filter if it is a column which is used as alias.
        if (prewhere_info && prewhere_info->remove_prewhere_column)
            if (required_columns.end() != std::find(required_columns.begin(), required_columns.end(), prewhere_info->prewhere_column_name))
                prewhere_info->remove_prewhere_column = false;

        /// Remove columns which will be added by prewhere.
        std::erase_if(required_columns, [&](const String & name) { return required_columns_after_prewhere_set.contains(name); });

        if (prewhere_info)
        {
            /// Don't remove columns which are needed to be aliased.
            for (const auto & name : required_columns)
                prewhere_info->prewhere_actions->tryRestoreColumn(name);

            /// Add physical columns required by prewhere actions.
            for (const auto & column : required_columns_from_prewhere)
                if (!required_aliases_from_prewhere.contains(column))
                    if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                        required_columns.push_back(column);
        }
    }
}

void InterpreterSelectQuery::executeFetchColumns(QueryProcessingStage::Enum processing_stage, QueryPlan & query_plan)
{
    auto & query = getSelectQuery();
    const Settings & settings = context->getSettingsRef();

    /// Optimization for trivial query like SELECT count() FROM table.
    bool optimize_trivial_count =
        syntax_analyzer_result->optimize_trivial_count
        && !syntax_analyzer_result->streaming
        && (settings.max_parallel_replicas <= 1)
        && storage
        && !row_policy_filter
        && processing_stage == QueryProcessingStage::FetchColumns
        && query_analyzer->hasAggregation()
        && (query_analyzer->aggregates().size() == 1)
        && typeid_cast<const AggregateFunctionCount *>(query_analyzer->aggregates()[0].function.get());

    if (optimize_trivial_count)
    {
        const auto & desc = query_analyzer->aggregates()[0];
        const auto & func = desc.function;
        std::optional<UInt64> num_rows{};

        if (!query.prewhere() && !query.where() && !context->getCurrentTransaction())
        {
            num_rows = storage->totalRows(settings);
        }
        else // It's possible to optimize count() given only partition predicates
        {
            SelectQueryInfo temp_query_info;
            temp_query_info.query = query_ptr;
            temp_query_info.syntax_analyzer_result = syntax_analyzer_result;
            temp_query_info.prepared_sets = query_analyzer->getPreparedSets();

            num_rows = storage->totalRowsByPartitionPredicate(temp_query_info, context);
        }

        if (num_rows)
        {
            const AggregateFunctionCount & agg_count = static_cast<const AggregateFunctionCount &>(*func);

            /// We will process it up to "WithMergeableState".
            std::vector<char> state(agg_count.sizeOfData());
            AggregateDataPtr place = state.data();

            agg_count.create(place);
            SCOPE_EXIT_MEMORY_SAFE(agg_count.destroy(place));

            agg_count.set(place, *num_rows);

            auto column = ColumnAggregateFunction::create(func);
            column->insertFrom(place);

            Block header = analysis_result.before_aggregation->getResultColumns();
            size_t arguments_size = desc.argument_names.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = header.getByName(desc.argument_names[j]).type;

            Block block_with_count{
                {std::move(column), std::make_shared<DataTypeAggregateFunction>(func, argument_types, desc.parameters), desc.column_name}};

            auto source = std::make_shared<SourceFromSingleChunk>(block_with_count);
            auto prepared_count = std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source)));
            prepared_count->setStepDescription("Optimized trivial count");
            query_plan.addStep(std::move(prepared_count));
            from_stage = QueryProcessingStage::WithMergeableState;
            analysis_result.first_stage = false;
            return;
        }
    }

    /// Limitation on the number of columns to read.
    /// It's not applied in 'only_analyze' mode, because the query could be analyzed without removal of unnecessary columns.
    if (!options.only_analyze && settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
        throw Exception(
            ErrorCodes::TOO_MANY_COLUMNS,
            "Limit for number of columns to read exceeded. Requested: {}, maximum: {}",
            required_columns.size(),
            settings.max_columns_to_read);

    /// General limit for the number of threads.
    size_t max_threads_execute_query = settings.max_threads;

    /** With distributed query processing, almost no computations are done in the threads,
     *  but wait and receive data from remote servers.
     *  If we have 20 remote servers, and max_threads = 8, then it would not be very good
     *  connect and ask only 8 servers at a time.
     *  To simultaneously query more remote servers,
     *  instead of max_threads, max_distributed_connections is used.
     */
    bool is_remote = false;
    if (storage && storage->isRemote())
    {
        is_remote = true;
        max_threads_execute_query = max_streams = settings.max_distributed_connections;
    }

    UInt64 max_block_size = settings.max_block_size;

    auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context);

    /** Optimization - if not specified DISTINCT, WHERE, GROUP, HAVING, ORDER, JOIN, LIMIT BY, WITH TIES
     *  but LIMIT is specified, and limit + offset < max_block_size,
     *  then as the block size we will use limit + offset (not to read more from the table than requested),
     *  and also set the number of threads to 1.
     */
    if (!query.distinct
        && !query.limit_with_ties
        && !query.prewhere()
        && !query.where()
        && !query.groupBy()
        && !query.having()
        && !query.orderBy()
        && !query.limitBy()
        && !query.join()
        && !query_analyzer->hasAggregation()
        && !query_analyzer->hasWindow()
        && query.limitLength()
        && limit_length <= std::numeric_limits<UInt64>::max() - limit_offset
        && limit_length + limit_offset < max_block_size)
    {
        max_block_size = std::max<UInt64>(1, limit_length + limit_offset);
        max_threads_execute_query = max_streams = 1;
    }

    if (!max_block_size)
        throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

    auto local_limits = getStorageLimits(*context, options);
    storage_limits.emplace_back(local_limits);

    /// Initialize the initial data streams to which the query transforms are superimposed. Table or subquery or prepared input?
    if (query_plan.isInitialized())
    {
        /// Prepared input.
    }
    else if (interpreter_subquery)
    {
        /// Subquery.
        ASTPtr subquery = extractTableExpression(query, 0);
        if (!subquery)
            throw Exception("Subquery expected", ErrorCodes::LOGICAL_ERROR);

        /// proton: starts.
        Streaming::rewriteSubquery(subquery->as<ASTSelectWithUnionQuery &>(), query_info);
        /// proton: ends.

        interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery, getSubqueryContext(context),
            options.copy().subquery().noModify(), required_columns);

        interpreter_subquery->addStorageLimits(storage_limits);

        if (query_analyzer->hasAggregation())
            interpreter_subquery->ignoreWithTotals();

        interpreter_subquery->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(context);
    }
    else if (storage)
    {
        /// Table.
        if (max_streams == 0)
            max_streams = 1;

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads.
        if (max_streams > 1 && !is_remote)
            max_streams = static_cast<size_t>(max_streams * settings.max_streams_to_max_threads_ratio);

        auto & prewhere_info = analysis_result.prewhere_info;

        if (prewhere_info)
            query_info.prewhere_info = prewhere_info;

        /// Create optimizer with prepared actions.
        /// Maybe we will need to calc input_order_info later, e.g. while reading from StorageMerge.
        if ((analysis_result.optimize_read_in_order || analysis_result.optimize_aggregation_in_order)
            && (!query_info.projection || query_info.projection->complete))
        {
            if (analysis_result.optimize_read_in_order)
            {
                if (query_info.projection)
                {
                    query_info.projection->order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                        // TODO Do we need a projection variant for this field?
                        analysis_result.order_by_elements_actions,
                        getSortDescription(query, context),
                        query_info.syntax_analyzer_result);
                }
                else
                {
                    query_info.order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                        analysis_result.order_by_elements_actions, getSortDescription(query, context), query_info.syntax_analyzer_result);
                }
            }
            else
            {
                if (query_info.projection)
                {
                    query_info.projection->order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                        query_info.projection->group_by_elements_actions,
                        getSortDescriptionFromGroupBy(query),
                        query_info.syntax_analyzer_result);
                }
                else
                {
                    query_info.order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                        analysis_result.group_by_elements_actions, getSortDescriptionFromGroupBy(query), query_info.syntax_analyzer_result);
                }
            }

            /// If we don't have filtration, we can pushdown limit to reading stage for optimizations.
            UInt64 limit = (query.hasFiltration() || query.groupBy()) ? 0 : getLimitForSorting(query, context);
            if (query_info.projection)
                query_info.projection->input_order_info
                    = query_info.projection->order_optimizer->getInputOrder(query_info.projection->desc->metadata, context, limit);
            else
                query_info.input_order_info = query_info.order_optimizer->getInputOrder(metadata_snapshot, context, limit);
        }

        query_info.storage_limits = std::make_shared<StorageLimitsList>(storage_limits);

        query_info.settings_limit_offset_done = options.settings_limit_offset_done;

        /// proton: starts.
        /// need replay operation
        if (settings.replay_speed > 0)
        {
            /// So far, only support append-only stream (or proxyed)
            StorageStream * storagestream = nullptr;
            if (Streaming::isAppendStorage(storage->dataStreamSemantic()))
            {
                if (const auto * proxy = storage->as<Streaming::ProxyStream>())
                {
                    const auto & proxyed = proxy->getProxyStorageOrSubquery();
                    const auto * nested_storage = std::get_if<StoragePtr>(&proxyed);
                    if (nested_storage)
                        storagestream = (*nested_storage)->as<StorageStream>();
                }
                else
                    storagestream = storage->as<StorageStream>();

                if (!storagestream)
                    throw Exception("Replay Stream is only support append-only stream", ErrorCodes::NOT_IMPLEMENTED);
            }
            assert(storagestream);

            if (std::ranges::none_of(required_columns, [](const auto & name) { return name == ProtonConsts::RESERVED_APPEND_TIME; }))
                required_columns.emplace_back(ProtonConsts::RESERVED_APPEND_TIME);

            if (std::ranges::none_of(
                    required_columns, [](const auto & name) { return name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID; }))
                required_columns.emplace_back(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);

            storage->read(
                query_plan, required_columns, storage_snapshot, query_info, context, processing_stage, max_block_size, max_streams);

            auto replay_step = std::make_unique<Streaming::ReplayStreamStep>(
                query_plan.getCurrentDataStream(), settings.replay_speed, (storagestream)->getLastSNs());
            replay_step->setStepDescription("Replay Stream");
            query_plan.addStep(std::move(replay_step));
        }
        else
            storage->read(
                query_plan, required_columns, storage_snapshot, query_info, context, processing_stage, max_block_size, max_streams);
        /// proton: ends.

        if (context->hasQueryContext() && !options.is_internal)
        {
            const String view_name{};
            auto local_storage_id = storage->getStorageID();
            context->getQueryContext()->addQueryAccessInfo(
                backQuoteIfNeed(local_storage_id.getDatabaseName()),
                local_storage_id.getFullTableName(),
                required_columns,
                query_info.projection ? query_info.projection->desc->name : "",
                view_name);
        }

        /// Create step which reads from empty source if storage has no data.
        if (!query_plan.isInitialized())
        {
            auto header = storage_snapshot->getSampleBlockForColumns(required_columns);
            addEmptySourceToQueryPlan(query_plan, header, query_info, context);
        }
    }
    else
        throw Exception("Logical error in InterpreterSelectQuery: nowhere to read", ErrorCodes::LOGICAL_ERROR);

    /// Specify the number of threads only if it wasn't specified in storage.
    ///
    /// But in case of remote query and prefer_localhost_replica=1 (default)
    /// The inner local query (that is done in the same process, without
    /// network interaction), it will setMaxThreads earlier and distributed
    /// query will not update it.
    if (!query_plan.getMaxThreads() || is_remote)
        query_plan.setMaxThreads(max_threads_execute_query);

    /// Aliases in table declaration.
    if (processing_stage == QueryProcessingStage::FetchColumns && alias_actions)
    {
        auto table_aliases = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), alias_actions);
        table_aliases->setStepDescription("Add stream aliases");
        query_plan.addStep(std::move(table_aliases));
    }
}


void InterpreterSelectQuery::executeWhere(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter)
{
    auto where_step = std::make_unique<FilterStep>(
        query_plan.getCurrentDataStream(), expression, getSelectQuery().where()->getColumnName(), remove_filter);

    where_step->setStepDescription("WHERE");
    query_plan.addStep(std::move(where_step));
}

/// proton : starts
void InterpreterSelectQuery::executeLightShuffling(QueryPlan & query_plan)
{
    auto key_positions = keyPositions(query_plan.getCurrentDataStream().header, getShuffleByColumns(getSelectQuery()));
    query_plan.addStep(std::make_unique<LightShufflingStep>(
        query_plan.getCurrentDataStream(), std::move(key_positions), context->getSettingsRef().max_threads.value));

    light_shuffled = true;
}
/// proton : ends

static Aggregator::Params getAggregatorParams(
    const ASTPtr & query_ptr,
    const SelectQueryExpressionAnalyzer & query_analyzer,
    const Context & context,
    const Block & current_data_stream_header,
    const ColumnNumbers & keys,
    const AggregateDescriptions & aggregates,
    bool overflow_row, const Settings & settings,
    size_t group_by_two_level_threshold, size_t group_by_two_level_threshold_bytes)
{
    const auto stats_collecting_params = Aggregator::Params::StatsCollectingParams(
        query_ptr,
        settings.collect_hash_table_stats_during_aggregation,
        settings.max_entries_for_hash_table_stats,
        settings.max_size_to_preallocate_for_aggregation);

    return Aggregator::Params
    {
        current_data_stream_header,
        keys,
        aggregates,
        overflow_row,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        group_by_two_level_threshold,
        group_by_two_level_threshold_bytes,
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set
            || (settings.empty_result_for_aggregation_by_constant_keys_on_empty_set && keys.empty()
                && query_analyzer.hasConstAggregationKeys()),
        context.getTemporaryVolume(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression,
        Block{},
        stats_collecting_params
    };
}

static GroupingSetsParamsList getAggregatorGroupingSetsParams(
    const SelectQueryExpressionAnalyzer & query_analyzer,
    const Block & header_before_aggregation,
    const ColumnNumbers & all_keys
)
{
    GroupingSetsParamsList result;
    if (query_analyzer.useGroupingSetKey())
    {
        auto const & aggregation_keys_list = query_analyzer.aggregationKeysList();

        ColumnNumbersList grouping_sets_with_keys;
        ColumnNumbersList missing_columns_per_set;

        for (const auto & aggregation_keys : aggregation_keys_list)
        {
            ColumnNumbers keys;
            std::unordered_set<size_t> keys_set;
            for (const auto & key : aggregation_keys)
            {
                keys.push_back(header_before_aggregation.getPositionByName(key.name));
                keys_set.insert(keys.back());
            }

            ColumnNumbers missing_indexes;
            for (size_t i = 0; i < all_keys.size(); ++i)
            {
                if (!keys_set.contains(all_keys[i]))
                    missing_indexes.push_back(i);
            }
            result.emplace_back(std::move(keys), std::move(missing_indexes));
        }
    }
    return result;
}

void InterpreterSelectQuery::executeAggregation(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, bool final, InputOrderInfoPtr group_by_info)
{
    /// proton: starts
    if (isStreamingQuery())
    {
        executeStreamingAggregation(query_plan, expression, overflow_row, final);
        return;
    }
    /// proton: ends

    auto expression_before_aggregation = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), expression);
    expression_before_aggregation->setStepDescription("Before GROUP BY");
    query_plan.addStep(std::move(expression_before_aggregation));

    if (options.is_projection_query)
        return;

    const auto & header_before_aggregation = query_plan.getCurrentDataStream().header;
    AggregateDescriptions aggregates = query_analyzer->aggregates();
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header_before_aggregation.getPositionByName(name));

    const Settings & settings = context->getSettingsRef();

    ColumnNumbers keys;
    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_aggregation.getPositionByName(key.name));

    auto aggregator_params = getAggregatorParams(
        query_ptr,
        *query_analyzer,
        *context,
        header_before_aggregation,
        keys,
        aggregates,
        overflow_row,
        settings,
        settings.group_by_two_level_threshold,
        settings.group_by_two_level_threshold_bytes);

    auto grouping_sets_params = getAggregatorGroupingSetsParams(*query_analyzer, header_before_aggregation, keys);

    SortDescription group_by_sort_description;

    if (group_by_info && settings.optimize_aggregation_in_order)
        group_by_sort_description = getSortDescriptionFromGroupBy(getSelectQuery());
    else
        group_by_info = nullptr;

    auto merge_threads = max_streams;
    auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
        ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
        : static_cast<size_t>(settings.max_threads);

    bool storage_has_evenly_distributed_read = storage && storage->hasEvenlyDistributedRead();

    auto aggregating_step = std::make_unique<AggregatingStep>(
        query_plan.getCurrentDataStream(),
        std::move(aggregator_params),
        std::move(grouping_sets_params),
        final,
        settings.max_block_size,
        settings.aggregation_in_order_max_block_bytes,
        merge_threads,
        temporary_data_merge_threads,
        storage_has_evenly_distributed_read,
        light_shuffled,
        std::move(group_by_info),
        std::move(group_by_sort_description));

    query_plan.addStep(std::move(aggregating_step));
}

void InterpreterSelectQuery::executeMergeAggregated(QueryPlan & query_plan, bool overflow_row, bool final, bool has_grouping_sets)
{
    /// If aggregate projection was chosen for table, avoid adding MergeAggregated.
    /// It is already added by storage (because of performance issues).
    /// TODO: We should probably add another one processing stage for storage?
    ///       WithMergeableStateAfterAggregation is not ok because, e.g., it skips sorting after aggregation.
    if (query_info.projection && query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
        return;

    executeMergeAggregatedImpl(
        query_plan,
        overflow_row,
        final,
        storage && storage->isRemote(),
        has_grouping_sets,
        context->getSettingsRef(),
        query_analyzer->aggregationKeys(),
        query_analyzer->aggregates());
}


void InterpreterSelectQuery::executeHaving(QueryPlan & query_plan, const ActionsDAGPtr & expression, bool remove_filter)
{
    auto having_step
        = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(), expression, getSelectQuery().having()->getColumnName(), remove_filter);

    having_step->setStepDescription("HAVING");
    query_plan.addStep(std::move(having_step));
}


void InterpreterSelectQuery::executeTotalsAndHaving(
    QueryPlan & query_plan, bool has_having, const ActionsDAGPtr & expression, bool remove_filter, bool overflow_row, bool final)
{
    const Settings & settings = context->getSettingsRef();

    auto totals_having_step = std::make_unique<TotalsHavingStep>(
        query_plan.getCurrentDataStream(),
        overflow_row,
        expression,
        has_having ? getSelectQuery().having()->getColumnName() : "",
        remove_filter,
        settings.totals_mode,
        settings.totals_auto_threshold,
        final);

    query_plan.addStep(std::move(totals_having_step));
}

void InterpreterSelectQuery::executeRollupOrCube(QueryPlan & query_plan, Modificator modificator)
{
    const auto & header_before_transform = query_plan.getCurrentDataStream().header;

    const Settings & settings = context->getSettingsRef();

    ColumnNumbers keys;
    for (const auto & key : query_analyzer->aggregationKeys())
        keys.push_back(header_before_transform.getPositionByName(key.name));

    auto params = getAggregatorParams(query_ptr, *query_analyzer, *context, header_before_transform, keys, query_analyzer->aggregates(), false, settings, 0, 0);
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), true, false);

    QueryPlanStepPtr step;
    if (modificator == Modificator::ROLLUP)
        step = std::make_unique<RollupStep>(query_plan.getCurrentDataStream(), std::move(transform_params));
    else if (modificator == Modificator::CUBE)
        step = std::make_unique<CubeStep>(query_plan.getCurrentDataStream(), std::move(transform_params));

    query_plan.addStep(std::move(step));
}

void InterpreterSelectQuery::executeExpression(QueryPlan & query_plan, const ActionsDAGPtr & expression, const std::string & description)
{
    if (!expression)
        return;

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), expression);

    expression_step->setStepDescription(description);
    query_plan.addStep(std::move(expression_step));
}

static bool windowDescriptionComparator(const WindowDescription * _left, const WindowDescription * _right)
{
    const auto & left = _left->full_sort_description;
    const auto & right = _right->full_sort_description;

    for (size_t i = 0; i < std::min(left.size(), right.size()); ++i)
    {
        if (left[i].column_name < right[i].column_name)
            return true;
        else if (left[i].column_name > right[i].column_name)
            return false;
        else if (left[i].direction < right[i].direction)
            return true;
        else if (left[i].direction > right[i].direction)
            return false;
        else if (left[i].nulls_direction < right[i].nulls_direction)
            return true;
        else if (left[i].nulls_direction > right[i].nulls_direction)
            return false;

        assert(left[i] == right[i]);
    }

    // Note that we check the length last, because we want to put together the
    // sort orders that have common prefix but different length.
    return left.size() > right.size();
}

static bool sortIsPrefix(const WindowDescription & _prefix,
    const WindowDescription & _full)
{
    const auto & prefix = _prefix.full_sort_description;
    const auto & full = _full.full_sort_description;

    if (prefix.size() > full.size())
        return false;

    for (size_t i = 0; i < prefix.size(); ++i)
    {
        if (full[i] != prefix[i])
            return false;
    }

    return true;
}

void InterpreterSelectQuery::executeStreamingWindow(QueryPlan & query_plan)
{
    assert(isStreamingQuery());

    if (!query_info.has_non_aggregate_over)
        return;

    assert(query_analyzer->windowDescriptions().size() == 1);
    for (const auto & [_, window_desc] : query_analyzer->windowDescriptions())
    {
        std::vector<WindowFunctionDescription> window_functions;
        window_functions.reserve(window_desc.window_functions.size());
        for (const auto & window_func : window_desc.window_functions)
        {
            if (window_func.function)
                window_functions.emplace_back(window_func);
        }

        auto window_step = std::make_unique<Streaming::WindowStep>(query_plan.getCurrentDataStream(), window_desc, window_functions);
        window_step->setStepDescription("Streaming window for substream '" + window_desc.window_name + "'");
        query_plan.addStep(std::move(window_step));
    }
}

void InterpreterSelectQuery::executeWindow(QueryPlan & query_plan)
{
    if (isStreamingQuery())
        return executeStreamingWindow(query_plan);

    // Try to sort windows in such an order that the window with the longest
    // sort description goes first, and all window that use its prefixes follow.
    std::vector<const WindowDescription *> windows_sorted;
    for (const auto & [_, window] : query_analyzer->windowDescriptions())
        windows_sorted.push_back(&window);

    ::sort(windows_sorted.begin(), windows_sorted.end(), windowDescriptionComparator);

    const Settings & settings = context->getSettingsRef();
    for (size_t i = 0; i < windows_sorted.size(); ++i)
    {
        const auto & window = *windows_sorted[i];

        // We don't need to sort again if the input from previous window already
        // has suitable sorting. Also don't create sort steps when there are no
        // columns to sort by, because the sort nodes are confused by this. It
        // happens in case of `over ()`.
        if (!window.full_sort_description.empty() && (i == 0 || !sortIsPrefix(window, *windows_sorted[i - 1])))
        {
            auto sorting_step = std::make_unique<SortingStep>(
                query_plan.getCurrentDataStream(),
                window.full_sort_description,
                settings.max_block_size,
                0 /* LIMIT */,
                SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
                settings.max_bytes_before_remerge_sort,
                settings.remerge_sort_lowered_memory_bytes_ratio,
                settings.max_bytes_before_external_sort,
                context->getTemporaryVolume(),
                settings.min_free_disk_space_for_temporary_data,
                settings.optimize_sorting_by_input_stream_properties);
            sorting_step->setStepDescription("Sorting for window '" + window.window_name + "'");
            query_plan.addStep(std::move(sorting_step));
        }

        auto window_step = std::make_unique<WindowStep>(query_plan.getCurrentDataStream(), window, window.window_functions);
        window_step->setStepDescription("Window step for window '" + window.window_name + "'");

        query_plan.addStep(std::move(window_step));
    }
}


void InterpreterSelectQuery::executeOrderOptimized(QueryPlan & query_plan, InputOrderInfoPtr input_sorting_info, UInt64 limit, SortDescription & output_order_descr)
{
    const Settings & settings = context->getSettingsRef();

    auto finish_sorting_step = std::make_unique<SortingStep>(
        query_plan.getCurrentDataStream(),
        input_sorting_info->order_key_prefix_descr,
        output_order_descr,
        settings.max_block_size,
        limit);

    query_plan.addStep(std::move(finish_sorting_step));
}

void InterpreterSelectQuery::executeOrder(QueryPlan & query_plan, InputOrderInfoPtr input_sorting_info)
{
    if (isStreamingQuery())
        return executeStreamingOrder(query_plan);

    auto & query = getSelectQuery();
    SortDescription output_order_descr = getSortDescription(query, context);
    UInt64 limit = getLimitForSorting(query, context);

    if (input_sorting_info)
    {
        /* Case of sorting with optimization using sorting key.
         * We have several threads, each of them reads batch of parts in direct
         *  or reverse order of sorting key using one input stream per part
         *  and then merge them into one sorted stream.
         * At this stage we merge per-thread streams into one.
         */
        executeOrderOptimized(query_plan, input_sorting_info, limit, output_order_descr);
        return;
    }

    const Settings & settings = context->getSettingsRef();

    /// Merge the sorted blocks.
    auto sorting_step = std::make_unique<SortingStep>(
        query_plan.getCurrentDataStream(),
        output_order_descr,
        settings.max_block_size,
        limit,
        SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
        settings.max_bytes_before_remerge_sort,
        settings.remerge_sort_lowered_memory_bytes_ratio,
        settings.max_bytes_before_external_sort,
        context->getTemporaryVolume(),
        settings.min_free_disk_space_for_temporary_data,
        settings.optimize_sorting_by_input_stream_properties);

    sorting_step->setStepDescription("Sorting for ORDER BY");
    query_plan.addStep(std::move(sorting_step));
}


void InterpreterSelectQuery::executeMergeSorted(QueryPlan & query_plan, const std::string & description)
{
    const auto & query = getSelectQuery();
    SortDescription sort_description = getSortDescription(query, context);
    const UInt64 limit = getLimitForSorting(query, context);
    const auto max_block_size = context->getSettingsRef().max_block_size;

    auto merging_sorted = std::make_unique<SortingStep>(query_plan.getCurrentDataStream(), std::move(sort_description), max_block_size, limit);
    merging_sorted->setStepDescription("Merge sorted streams " + description);
    query_plan.addStep(std::move(merging_sorted));
}


void InterpreterSelectQuery::executeProjection(QueryPlan & query_plan, const ActionsDAGPtr & expression)
{
    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), expression);
    projection_step->setStepDescription("Projection");
    query_plan.addStep(std::move(projection_step));
}


void InterpreterSelectQuery::executeDistinct(QueryPlan & query_plan, bool before_order, Names columns, bool pre_distinct)
{
    auto & query = getSelectQuery();
    if (query.distinct)
    {
        const Settings & settings = context->getSettingsRef();

        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context);
        UInt64 limit_for_distinct = 0;

        /// If after this stage of DISTINCT ORDER BY is not executed,
        /// then you can get no more than limit_length + limit_offset of different rows.
        if ((!query.orderBy() || !before_order) && limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
            limit_for_distinct = limit_length + limit_offset;

        SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

        auto distinct_step = std::make_unique<DistinctStep>(
            query_plan.getCurrentDataStream(),
            limits,
            limit_for_distinct,
            columns,
            pre_distinct,
            settings.optimize_distinct_in_order);

        if (pre_distinct)
            distinct_step->setStepDescription("Preliminary DISTINCT");

        query_plan.addStep(std::move(distinct_step));
    }
}


/// Preliminary LIMIT - is used in every source, if there are several sources, before they are combined.
void InterpreterSelectQuery::executePreLimit(QueryPlan & query_plan, bool do_not_skip_offset)
{
    /// proton: starts.
    if (isStreamingQuery())
        return executeStreamingPreLimit(query_plan, do_not_skip_offset);
    /// proton: ends.

    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context);

        if (do_not_skip_offset)
        {
            if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
                return;

            limit_length += limit_offset;
            limit_offset = 0;
        }

        auto limit = std::make_unique<LimitStep>(query_plan.getCurrentDataStream(), limit_length, limit_offset);
        if (do_not_skip_offset)
            limit->setStepDescription("preliminary LIMIT (with OFFSET)");
        else
            limit->setStepDescription("preliminary LIMIT (without OFFSET)");

        query_plan.addStep(std::move(limit));
    }
}


void InterpreterSelectQuery::executeLimitBy(QueryPlan & query_plan)
{
    auto & query = getSelectQuery();
    if (!query.limitByLength() || !query.limitBy())
        return;

    Names columns;
    for (const auto & elem : query.limitBy()->children)
        columns.emplace_back(elem->getColumnName());

    UInt64 length = getLimitUIntValue(query.limitByLength(), context, "LIMIT");
    UInt64 offset = (query.limitByOffset() ? getLimitUIntValue(query.limitByOffset(), context, "OFFSET") : 0);

    auto limit_by = std::make_unique<LimitByStep>(query_plan.getCurrentDataStream(), length, offset, columns);
    query_plan.addStep(std::move(limit_by));
}

void InterpreterSelectQuery::executeWithFill(QueryPlan & query_plan)
{
    auto & query = getSelectQuery();
    if (query.orderBy())
    {
        SortDescription order_descr = getSortDescription(query, context);
        SortDescription fill_descr;
        for (auto & desc : order_descr)
        {
            if (desc.with_fill)
                fill_descr.push_back(desc);
        }

        if (fill_descr.empty())
            return;

        auto filling_step = std::make_unique<FillingStep>(query_plan.getCurrentDataStream(), std::move(fill_descr));
        query_plan.addStep(std::move(filling_step));
    }
}


void InterpreterSelectQuery::executeLimit(QueryPlan & query_plan)
{
    /// proton: starts.
    if (isStreamingQuery())
        return executeStreamingLimit(query_plan);
    /// proton: ends.

    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
    {
        /** Rare case:
          *  if there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
          *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
          *  because if you cancel the query, we will not get `totals` data from the remote server.
          *
          * Another case:
          *  if there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
          *  otherwise TOTALS is counted according to incomplete data.
          */
        bool always_read_till_end = false;

        if (query.group_by_with_totals && !query.orderBy())
            always_read_till_end = true;

        if (!query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(query))
            always_read_till_end = true;

        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, context);

        SortDescription order_descr;
        if (query.limit_with_ties)
        {
            if (!query.orderBy())
                throw Exception("LIMIT WITH TIES without ORDER BY", ErrorCodes::LOGICAL_ERROR);
            order_descr = getSortDescription(query, context);
        }

        auto limit = std::make_unique<LimitStep>(
                query_plan.getCurrentDataStream(),
                limit_length, limit_offset, always_read_till_end, query.limit_with_ties, order_descr);

        if (query.limit_with_ties)
            limit->setStepDescription("LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
}


void InterpreterSelectQuery::executeOffset(QueryPlan & query_plan)
{
    /// proton: starts.
    if (isStreamingQuery())
        return executeStreamingOffset(query_plan);
    /// proton: ends.

    auto & query = getSelectQuery();
    /// If there is not a LIMIT but an offset
    if (!query.limitLength() && query.limitOffset())
    {
        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, context);

        auto offsets_step = std::make_unique<OffsetStep>(query_plan.getCurrentDataStream(), limit_offset);
        query_plan.addStep(std::move(offsets_step));
    }
}

void InterpreterSelectQuery::executeExtremes(QueryPlan & query_plan)
{
    if (!context->getSettingsRef().extremes)
        return;

    auto extremes_step = std::make_unique<ExtremesStep>(query_plan.getCurrentDataStream());
    query_plan.addStep(std::move(extremes_step));
}

void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(QueryPlan & query_plan)
{
    addCreatingSetsStep(query_plan, prepared_sets, context);
}


void InterpreterSelectQuery::ignoreWithTotals()
{
    getSelectQuery().group_by_with_totals = false;
}


void InterpreterSelectQuery::initSettings()
{
    auto & query = getSelectQuery();
    if (query.settings())
        InterpreterSetQuery(query.settings(), context).executeForCurrentContext();

    /// auto & client_info = context->getClientInfo();
    /// auto min_major = DBMS_MIN_MAJOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD;
    /// auto min_minor = DBMS_MIN_MINOR_VERSION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD;

    /// if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
    ///    std::forward_as_tuple(client_info.connection_client_version_major, client_info.connection_client_version_minor) < std::forward_as_tuple(min_major, min_minor))
    /// {
    ///    /// Disable two-level aggregation due to version incompatibility.
    ///    context->setSetting("group_by_two_level_threshold", Field(0));
    ///    context->setSetting("group_by_two_level_threshold_bytes", Field(0));
    /// }
}

/// proton: starts
void InterpreterSelectQuery::executeStreamingOrder(QueryPlan & query_plan)
{
    const Settings & settings = context->getSettingsRef();

    auto & query = getSelectQuery();
    SortDescription output_order_descr = getSortDescription(query, context);
    UInt64 limit = getLimitForSorting(query, context);

    auto sorting_step = std::make_unique<Streaming::SortingStep>(
        query_plan.getCurrentDataStream(),
        output_order_descr,
        settings.max_block_size,
        limit,
        SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode),
        settings.max_bytes_before_remerge_sort,
        settings.remerge_sort_lowered_memory_bytes_ratio,
        settings.max_bytes_before_external_sort,
        context->getTemporaryVolume(),
        settings.min_free_disk_space_for_temporary_data);

    sorting_step->setStepDescription("Streaming Sorting for ORDER BY");
    query_plan.addStep(std::move(sorting_step));
}

void InterpreterSelectQuery::executeStreamingAggregation(
    QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, bool final)
{
    assert(isStreamingQuery());

    auto expression_before_aggregation = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), expression);
    expression_before_aggregation->setStepDescription("Before GROUP BY");
    query_plan.addStep(std::move(expression_before_aggregation));

    if (options.is_projection_query)
        return;

    auto streaming_group_by = Streaming::Aggregator::Params::GroupBy::OTHER;

    const auto & header_before_aggregation = query_plan.getCurrentDataStream().header;
    ColumnNumbers keys;

    ssize_t delta_col_pos = data_stream_semantic_pair.isChangelogInput()
        ? header_before_aggregation.getPositionByName(ProtonConsts::RESERVED_DELTA_FLAG)
        : -1;

    size_t window_keys_num = 0;

    for (const auto & key : query_analyzer->aggregationKeys())
    {
        /// In case when `select count() from (select 1 as window_start, 2 as window_end from test) group by window_start, window_end`
        /// There is no window, so `window_start/window_end` are just normal group by keys.
        if (query_info.streaming_window_params)
        {
            if ((key.name == ProtonConsts::STREAMING_WINDOW_END) && (isDate(key.type) || isDateTime(key.type) || isDateTime64(key.type)))
            {
                keys.insert(keys.begin(), header_before_aggregation.getPositionByName(key.name));
                streaming_group_by = Streaming::Aggregator::Params::GroupBy::WINDOW_END;
                ++window_keys_num;
                continue;
            }
            else if ((key.name == ProtonConsts::STREAMING_WINDOW_START) && (isDate(key.type) || isDateTime(key.type) || isDateTime64(key.type)))
            {
                keys.insert(keys.begin(), header_before_aggregation.getPositionByName(key.name));
                streaming_group_by = Streaming::Aggregator::Params::GroupBy::WINDOW_START;
                ++window_keys_num;
                continue;
            }
        }

        keys.push_back(header_before_aggregation.getPositionByName(key.name));
    }

    AggregateDescriptions aggregates = query_analyzer->aggregates();
    /// Convert window over aggregate descriptions to regular Aggregate descriptions
    if (query_info.has_aggregate_over)
    {
        /// Window over aggregates are not compatible with regular aggregates
        /// so they can't coexist at the same level in one SQL.
        assert(aggregates.empty());
        assert(query_analyzer->windowDescriptions().size() <= 1);
        for (const auto & [_, window_desc] : query_analyzer->windowDescriptions())
        {
            for (const auto & window_func : window_desc.window_functions)
            {
                if (!window_func.aggregate_function)
                    continue;

                AggregateDescription aggr_desc;
                aggr_desc.function = window_func.aggregate_function;
                aggr_desc.argument_names = window_func.argument_names;
                aggr_desc.column_name = window_func.column_name;
                aggregates.emplace_back(std::move(aggr_desc));
            }
        }
    }

    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header_before_aggregation.getPositionByName(name));

    if (has_user_defined_emit_strategy)
    {
        if (aggregates.size() > 1)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "User defined aggregation function with emit strategy shouldn't be used together with other aggregation function");

        if (windowType() != Streaming::WindowType::NONE)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "User defined aggregation function with emit strategy shouldn't be used together with streaming window");

        assert(streaming_group_by == Streaming::Aggregator::Params::GroupBy::OTHER);
        streaming_group_by = Streaming::Aggregator::Params::GroupBy::USER_DEFINED;
    }

    const Settings & settings = context->getSettingsRef();

    /// TODO: support more overflow mode
    if (unlikely(settings.group_by_overflow_mode != OverflowMode::THROW))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Streaming aggregatation group by overflow mode '{}' is not implemented",
            magic_enum::enum_name(settings.group_by_overflow_mode.value));

    auto tracking_updates_type = Streaming::TrackingUpdatesType::None;
    if (data_stream_semantic_pair.isChangelogOutput())
        tracking_updates_type = Streaming::TrackingUpdatesType::UpdatesWithRetract;
    /// TODO: A optimization for `emit on update`, we don't need to track updates and just directly convert each input (fast in and fast out)
    else if (Streaming::AggregatingHelper::onlyEmitUpdates(emit_mode))
        tracking_updates_type = Streaming::TrackingUpdatesType::Updates;

    Streaming::Aggregator::Params params(
        header_before_aggregation,
        keys,
        aggregates,
        overflow_row,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        settings.group_by_two_level_threshold,
        settings.group_by_two_level_threshold_bytes,
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set
            || (settings.empty_result_for_aggregation_by_constant_keys_on_empty_set && keys.empty()
                && query_analyzer->hasConstAggregationKeys()),
        context->getTemporaryVolume(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression,
        {},
        shouldKeepAggregateState(),
        settings.keep_windows,
        streaming_group_by,
        delta_col_pos,
        window_keys_num,
        query_info.streaming_window_params,
        tracking_updates_type);

    auto merge_threads = max_streams;
    auto temporary_data_merge_threads = settings.aggregation_memory_efficient_merge_threads
        ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
        : static_cast<size_t>(settings.max_threads);

    /// There are two substream categories:
    /// 1) `parition by`: calculating substream with substream ID (The data have been shuffled by `ShufflingTransform`)
    /// 2) `shuffle by`: calculating light substream without substream ID (The data have been shuffled by `LightShufflingTransform`)
    if (query_info.hasPartitionByKeys() || light_shuffled)
        query_plan.addStep(std::make_unique<Streaming::AggregatingStepWithSubstream>(
            query_plan.getCurrentDataStream(), std::move(params), final, emit_version, data_stream_semantic_pair.isChangelogOutput(), emit_mode));
    else
        query_plan.addStep(std::make_unique<Streaming::AggregatingStep>(
            query_plan.getCurrentDataStream(), std::move(params), final, merge_threads, temporary_data_merge_threads, emit_version, data_stream_semantic_pair.isChangelogOutput(), emit_mode));
}

/// Resolve input / output data stream semantic.
/// Output data stream semantic depends on the current layer of query (its inputs) as well as the parent's SELECT query
/// Basically parent SELECT pushes `has_aggregates / has_join` down to subquery, and subquery then decides its
/// output semantic according its SELECT and the data inputs
void InterpreterSelectQuery::resolveDataStreamSemantic(const JoinedTables & joined_tables)
{
    if (!isStreamingQuery() || context->getSettingsRef().enforce_append_only.value)
        /// Default append
        return;

    /// Resolve right data stream semantic
    const auto & tables_with_columns = joined_tables.tablesWithColumns();
    assert(tables_with_columns.size() <= 2);
    if (tables_with_columns.size() == 1)
    {
        data_stream_semantic_pair = Streaming::calculateDataStreamSemantic(
            tables_with_columns[0].output_data_stream_semantic, {}, {}, current_select_has_aggregates, query_info);
    }
    else if (tables_with_columns.size() == 2)
    {
        /// auto join_strictness = Streaming::analyzeJoinKindAndStrictness(getSelectQuery(), context->getSettingsRef().join_default_strictness);
        data_stream_semantic_pair = Streaming::calculateDataStreamSemantic(
            tables_with_columns[0].output_data_stream_semantic,
            tables_with_columns[1].output_data_stream_semantic,
            current_select_join_kind_and_strictness,
            current_select_has_aggregates,
            query_info);
    }
    else
    {
        assert(tables_with_columns.empty());
        /// Default append
    }
}

std::set<String> InterpreterSelectQuery::getGroupByColumns() const
{
    std::set<String> group_by_columns;
    ASTSelectQuery & query = query_ptr->as<ASTSelectQuery &>();
    if (query.groupBy())
    {
        for (const auto & elem : query.groupBy()->children)
        {
            auto col = elem->getColumnName();
            /// skip the useless '_tp_time'
            if (col == "_tp_time")
                continue;

            /// Insert the alias name
            bool is_alias = false;
            if (syntax_analyzer_result)
            {
                for (const auto & [alias, ast] : syntax_analyzer_result->aliases)
                {
                    if (ast->getColumnName() == col)
                    {
                        group_by_columns.insert(alias);
                        is_alias = true;
                        break;
                    }
                }
            }

            if (!is_alias)
                group_by_columns.insert(col);
        }
    }

    return group_by_columns;
}

bool InterpreterSelectQuery::hasStreamingWindowFunc() const
{
    return query_info.streaming_window_params != nullptr;
}

Streaming::WindowType InterpreterSelectQuery::windowType() const
{
    if (storage)
    {
        if (auto * proxy = storage->as<Streaming::ProxyStream>())
        {
            return proxy->windowType();
        }
    }

    return Streaming::WindowType::NONE;
}

bool InterpreterSelectQuery::hasStreamingGlobalAggregation() const
{
    return isStreamingQuery() && hasAggregation() && !hasStreamingWindowFunc();
}

bool InterpreterSelectQuery::shouldKeepAggregateState() const
{
    if (!isStreamingQuery())
        return false;

    if (hasStreamingWindowFunc())
        return true;

    if (hasStreamingGlobalAggregation())
    {
        if (data_stream_semantic_pair.isChangelogInput())
            return true;

        /// When global aggregation is over global aggregation, we don't need keep the aggregation
        /// state of the outer aggregator.
        /// For example, for query `SELECT sum(s) FROM (SELECT sum(i) FROM kafka_stream);`
        /// we can't keep the aggregation state of `sum(s)` when inner subquery emits
        if (interpreter_subquery && interpreter_subquery->hasStreamingGlobalAggregation())
            return false;

        /// subquery
        if (storage)
        {
            if (auto * proxy = storage->as<Streaming::ProxyStream>())
                if (proxy->hasStreamingGlobalAggregation())
                    return false;
        }

        return true;
    }

    return false;
}

void InterpreterSelectQuery::finalCheckAndOptimizeForStreamingQuery()
{
    if (isStreamingQuery())
    {
        /// For now, for the following scenarios, we disable backfill from historic data store
        /// 1) User select some virtual columns which is only available in streaming store, like `_tp_sn`, `_tp_index_time`
        /// 2) Seek by streaming store sequence number
        /// 3) Replaying a stream.
        /// TODO, ideally we shall check if historical data store has `_tp_sn` etc columns, if they have, we can backfill from
        /// the historical data store as well technically. This will be a future enhancement.
        const auto & settings = context->getSettingsRef();
        if (settings.enable_backfill_from_historical_store.value)
        {
            bool has_streaming_only_virtual_columns = std::ranges::any_of(required_columns, [](const auto & name) {
                return name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID || name == ProtonConsts::RESERVED_APPEND_TIME
                    || name == ProtonConsts::RESERVED_INGEST_TIME || name == ProtonConsts::RESERVED_PROCESS_TIME;
            });
            bool seek_by_sn = !query_info.seek_to_info->getSeekTo().empty() && !query_info.seek_to_info->isTimeBased()
                && query_info.seek_to_info->getSeekTo() != "earliest";
            if (has_streaming_only_virtual_columns || seek_by_sn || settings.replay_speed > 0)
                context->setSetting("enable_backfill_from_historical_store", false);
        }

        /// Usually, we don't care whether the backfilled data is in order. Excepts:
        /// 1) User require backfill data in order
        /// 2) User need window aggr emit result during backfill (it expects that process data in ascending event time)
        if (settings.emit_during_backfill.value && hasAggregation() && hasStreamingWindowFunc())
            context->setSetting("force_backfill_in_order", true);

        if (settings.force_backfill_in_order.value)
            query_info.require_in_order_backfill = true;
    }
    else
    {
        if (query_info.force_emit_changelog)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Emit changelog is only supported in streaming processing query");
    }

    if (hasStreamingWindowFunc())
    {
        bool has_win_col = false;
        for (const auto & window_col : ProtonConsts::STREAMING_WINDOW_COLUMN_NAMES)
        {
            if (std::find(required_columns.begin(), required_columns.end(), window_col) != required_columns.end())
            {
                has_win_col = true;
                break;
            }
        }

        if (!has_win_col)
            throw Exception(
                "Neither window_start nor window_end is referenced in the query, but streaming window function is used",
                ErrorCodes::WINDOW_COLUMN_NOT_REFERENCED);
    }

    checkUDA();
}

void InterpreterSelectQuery::buildShufflingQueryPlan(QueryPlan & query_plan)
{
    assert(isStreamingQuery());
    if (!query_info.hasPartitionByKeys())
        return;

    /// TODO: Support more shuffling rules
    /// 1) Group by keys
    /// 2) Sharding expr keys
    ///
    /// We like to limit the shuffling concurrency here
    /// 1) No more than number of inputs concurrency
    /// 2) If there is JavaScript UDA, limit the concurrency further
    size_t shuffle_output_streams = context->getSettingsRef().max_threads.value;
    size_t controlled_concurrency = context->getSettingsRef().javascript_uda_max_concurrency.value;
    if (query_info.has_javascript_uda)
    {
        shuffle_output_streams = std::min(shuffle_output_streams, controlled_concurrency);
        LOG_INFO(log, "Limit shuffling output stream to {} for JavaScript UDA", shuffle_output_streams);
    }
    shuffle_output_streams = shuffle_output_streams == 0 ? 1 : shuffle_output_streams;

    auto substream_key_positions = keyPositions(query_plan.getCurrentDataStream().header, query_info.partition_by_keys);
    query_plan.addStep(std::make_unique<Streaming::ShufflingStep>(
        query_plan.getCurrentDataStream(), std::move(substream_key_positions), shuffle_output_streams));
}

void InterpreterSelectQuery::buildWatermarkQueryPlan(QueryPlan & query_plan)
{
    assert(isStreamingQuery());
    auto params = std::make_shared<Streaming::WatermarkStamperParams>(
        query_info.query, query_info.syntax_analyzer_result, query_info.streaming_window_params);

    emit_mode = params->mode; /// saved it to be used for streaming aggregating step

    bool skip_stamping_for_backfill_data = !context->getSettingsRef().emit_during_backfill.value;

    if (query_info.hasPartitionByKeys())
        query_plan.addStep(std::make_unique<Streaming::WatermarkStepWithSubstream>(
            query_plan.getCurrentDataStream(), std::move(params), skip_stamping_for_backfill_data, log));
    else
        query_plan.addStep(std::make_unique<Streaming::WatermarkStep>(
            query_plan.getCurrentDataStream(), std::move(params), skip_stamping_for_backfill_data, log));
}

void InterpreterSelectQuery::buildStreamingProcessingQueryPlanBeforeJoin(QueryPlan & query_plan)
{
    if (!isStreamingQuery() || query_info.has_non_aggregate_over || has_user_defined_emit_strategy || !hasStreamingWindowFunc())
        return;

    if (query_info.hasPartitionByKeys())
    {
        /// FIXME: Refactor watermark for substream
        /// Normally, we should execute `partition by` after join, but current implementation of watermark
        /// over substream depends on shuffled data.
        /// So we allow do shuffling ahead for some special cases:
        /// 1) Non-join query
        /// 2) Streaming join table, and all partition by key columns are from left stream
        if (analysis_result.hasJoin())
        {
            bool is_stream_join_table = !typeid_cast<Streaming::IHashJoin *>(analysis_result.join.get());
            /// If all `partition by` key columns are from left stream
            const auto & header = query_plan.getCurrentDataStream().header;
            bool only_shuffling_left_stream = std::ranges::all_of(query_info.partition_by_keys, [&](const auto & key) { return header.has(key); });
            if (!(is_stream_join_table && only_shuffling_left_stream))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "The join query with partition by clause doesn't support to use '{}' window function",
                    magic_enum::enum_name(query_info.streaming_window_params->type));
        }

        buildShufflingQueryPlan(query_plan);
        shuffled_before_join = true;
    }

    buildWatermarkQueryPlan(query_plan);
}

void InterpreterSelectQuery::buildStreamingProcessingQueryPlanAfterJoin(QueryPlan & query_plan)
{
    if (!isStreamingQuery())
        return;

    /// If `Shuffle` step is already inserted in `buildStreamingProcessingQueryPlanBeforeJoin`, skip it
    if (!shuffled_before_join)
        buildShufflingQueryPlan(query_plan);

    if (has_user_defined_emit_strategy || !hasStreamingGlobalAggregation())
        return;

    /// An optimizing path, skip duplicate periodic watermark.
    /// But if there is join query, we must establish new periodic watermark for joined data
    if (!analysis_result.hasJoin())
    {
        /// CTE subquery
        if (storage)
        {
            if (auto * proxy = storage->as<Streaming::ProxyStream>())
            {
                if (proxy->hasStreamingGlobalAggregation())
                    return;
            }
        }

        /// nested global aggregation
        if (interpreter_subquery && interpreter_subquery->hasStreamingGlobalAggregation())
            return;
    }

    /// Build global periodic watermark
    buildWatermarkQueryPlan(query_plan);
}

void InterpreterSelectQuery::checkEmitVersion()
{
    if (emit_version)
    {
        bool streaming = isStreamingQuery();
        /// emit_version() shall be used along with aggregation only
        if (streaming && syntax_analyzer_result->aggregates.empty() && !syntax_analyzer_result->has_group_by)
            throw Exception(ErrorCodes::UNSUPPORTED, "emit_version() shall be only used along with streaming aggregations");
        else if (!streaming)
            throw Exception(ErrorCodes::UNSUPPORTED, "emit_version() shall be only used in streaming query");
    }
}

void InterpreterSelectQuery::handleSeekToSetting()
{
    const auto & seek_to = context->getSettingsRef().seek_to.value;

    assert(!query_info.seek_to_info);
    query_info.seek_to_info = std::make_shared<SeekToInfo>(seek_to);

    if (!isStreamingQuery() && !seek_to.empty())
    {
        LOG_WARNING(log, "It doesn't support `seek_to` setting in historical table query, so ignored.");
        return;
    }

    if (query_info.seek_to_info->isTimeBased())
    {
        const auto & seek_points = query_info.seek_to_info->getSeekPoints();
        if (seek_points.size() != 1)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "It doesn't support time based `seek_to` settings for multiple shards");

        /// If the storage can do accurate seek_to (for example, Kafka external streams) no extra work is needed.
        /// Otherwise, the WHERE predicates of SELECT query will be rewritten by adding a filter for filtering
        /// records by `_tp_time`. For example: `SELECT * FROM stream SETTINGS seek_to='2022-01-01 00:01:01'` will
        /// be rewritten to `SELECT * FROM stream WHERE _tp_time >= '2022-01-01 00:01:01'`.
        if (storage && !storage->supportsAccurateSeekTo())
            addEventTimePredicate(getSelectQuery(), seek_points[0]);
    }
    else
    {
        /// Do nothing here. For sequence number based seek_to, it will be handled in StorageStream directly
    }
}

void InterpreterSelectQuery::analyzeEventPredicateAsSeekTo(const JoinedTables & joined_tables)
{
    /// If a streaming query already has `seek_to` query setting like
    /// `SELECT * FROM my_stream WHERE _tp_time > '2023-01-01 00:01:01' SETTINGS seek_to=2022-01-01 00:01:01`.
    /// `seek_to` in query setting dominates event time predicate in where clause.
    /// We choose this design because we like query backward compatibility and `seek_to` to be an internal workaround to do a streaming store rewinding.
    if (!isStreamingQuery() || !context->getSettingsRef().seek_to.value.empty())
        return;

    Streaming::EventPredicateVisitor::Data data(getSelectQuery(), joined_tables.tablesWithColumns(), context);
    Streaming::EventPredicateVisitor(data).visit(query_ptr);

    /// Try set seek to info for the left table (if exists, no need analyzing for the second time)
    /// For example: select s from stream as a inner join stream as b using (i) where a._tp_time >= earliest_ts() and b._tp_time >= earliest_ts()
    /// After first analyze, the where clause `where a._tp_time >= earliest_ts() and b._tp_time >= earliest_ts()` was optimized (where true and true => removed)
    /// So we don't need analyze again.
    if (!query_info.seek_to_info || query_info.seek_to_info->getSeekTo().empty())
        if (auto seek_to_info = data.tryGetSeekToInfoForLeftStream())
            query_info.seek_to_info = std::move(seek_to_info);

    /// Try set seek to info for the right table (if exists, no need analyzing for the second time)
    if (!query_info.seek_to_info_of_right_stream || query_info.seek_to_info_of_right_stream->getSeekTo().empty())
    {
        if (auto seek_to_info_of_right_stream = data.tryGetSeekToInfoForRightStream())
        {
            if (!query_analyzer->hasTableJoin())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown seek to info");

            query_info.seek_to_info_of_right_stream = std::move(seek_to_info_of_right_stream);
        }
    }
    query_analyzer->setSeekToInfoForJoinedTable(query_info.seek_to_info_of_right_stream);
}

bool InterpreterSelectQuery::isStreamingQuery() const
{
    if (is_streaming_query.has_value())
        return *is_streaming_query;

    /// We can simply determine the query type (stream or not) by the type of storage or subquery.
    /// Although `TreeRewriter` optimization may rewrite the subquery, it does not affect whether it is streaming
    /// And for now, we only look at the left stream even in a join case since we don't support
    /// `table join stream` case yet. When the left stream is streaming, then the whole query will be streaming.
    bool streaming = false;
    if (context->getSettingsRef().query_mode.value == "table")
        streaming = false; /// force table mode
    else if (storage)
        streaming = isStreamingStorage(storage, context);
    else if (interpreter_subquery)
        streaming = interpreter_subquery->isStreamingQuery();

    is_streaming_query = streaming;

    return streaming;
}

void InterpreterSelectQuery::checkAndPrepareStreamingFunctions()
{
    /// Prepare streaming version of the functions
    bool streaming = isStreamingQuery();
    Streaming::SubstituteStreamingFunctionVisitor::Data func_data(streaming, data_stream_semantic_pair.isChangelogInput());
    Streaming::SubstituteStreamingFunctionVisitor(func_data).visit(query_ptr);
    emit_version = func_data.emit_version;

    /// Prepare streaming window params
    if (storage)
    {
        if (auto * proxy = storage->as<Streaming::ProxyStream>())
        {
            if (auto window_desc = proxy->getStreamingWindowFunctionDescription())
            {
                query_info.streaming_window_params = Streaming::WindowParams::create(window_desc);
                if (data_stream_semantic_pair.isChangelogInput())
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "The window '{}' is not supported in changelog query processing",
                        magic_enum::enum_name(window_desc->type));
            }
        }
    }

    if (!streaming)
        return;

    /// Assign partition by for aggregate / stateful functions
    /// select sum(x), avg(x) from ... partition by id
    /// e.g. sum(x) -> sum(x) over(partition by id), avg(x) over(partition by id)
    PartitionByVisitor::Data partition_by_data;
    partition_by_data.context = context;
    PartitionByVisitor(partition_by_data).visit(query_ptr);

    /// Prepare streaming window functions
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query_ptr);

    if (!data.aggregates.empty() && !data.window_functions.empty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Window over aggregation is not compatible with non-window over aggregation in the same query");

    query_info.has_aggregate_over = !data.aggregate_overs.empty();
    query_info.has_non_aggregate_over = !data.non_aggregate_overs.empty();

    if (query_info.has_aggregate_over && query_info.has_non_aggregate_over)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Window over aggregation is not compatible with window over non-aggregation in the same query");

    String unique_window_name;
    for (const auto * function_node : data.window_functions)
    {
        assert(function_node->is_window_function);
        const auto & definition = function_node->window_definition->as<const ASTWindowDefinition &>();
        /// Not support follows syntax:
        /// 1) select func(...) OVER window_name from stream WINDOW window_name as (partition by ...)
        /// 2) select func(...) OVER (window_name) from stream WINDOW window_name as (partition by ...)
        if (!function_node->window_name.empty() || !definition.parent_window_name.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "No support that use predefined window '{}' in streaming queries",
                function_node->window_name.empty() ? definition.parent_window_name : function_node->window_name);

        // Not support frame in Streaming Window
        if (!definition.frame_is_default)
            throw Exception(
                ErrorCodes::UNSUPPORTED, "Window frame is not supported in streaming window over aggregation '{}'", function_node->name);

        if (definition.order_by)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Order by is not support in streaming window over aggregation '{}'", function_node->name);

        if (!definition.partition_by)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "'PARTITION BY' is required but missing in streaming window over aggregation '{}'",
                function_node->name);

        if (unique_window_name.empty())
        {
            unique_window_name = definition.getDefaultWindowName();
            auto & query = getSelectQuery();
            if (query.groupBy())
            {
                /// Append `PARTITION BY` keys to `GROUP BY` expression
                auto & group_exprs = query.groupBy()->children;
                for (const auto & column_ast : definition.partition_by->children)
                    group_exprs.emplace_back(column_ast->clone());
            }
            else if (!data.aggregate_overs.empty())
            {
                /// Use PARTITION BY keys as GROUP BY expression
                query.setExpression(ASTSelectQuery::Expression::GROUP_BY, definition.partition_by->clone());
            }

            /// Precached ahead partition keys before analyzing AST to avoid keys missing,
            /// in special case when `select i, max(i) over (partition by id) from test group by i`,
            /// it will be optimized to `select i, i from test group by i`
            if (query_info.has_aggregate_over || query_info.has_non_aggregate_over)
            {
                query_info.partition_by_keys.reserve(definition.partition_by->children.size());
                for (const auto & column_ast : definition.partition_by->children)
                    query_info.partition_by_keys.emplace_back(column_ast->getColumnName());
            }
        }
        else if (definition.getDefaultWindowName() != unique_window_name)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Streaming window over is required to have identical signature, but '{}' is not the same as '{}'",
                unique_window_name,
                definition.getDefaultWindowName());
    }
}

void InterpreterSelectQuery::checkUDA()
{
    for (const auto & aggr_func_desc : query_analyzer->aggregates())
    {
        if (aggr_func_desc.function->hasUserDefinedEmit())
            has_user_defined_emit_strategy = true;

        if (aggr_func_desc.function->udfType() == UDFType::Javascript)
            query_info.has_javascript_uda = true;
    }

    for (const auto & [_, window_desc] : query_analyzer->windowDescriptions())
    {
        for (const auto & window_func : window_desc.window_functions)
        {
            if (!window_func.aggregate_function)
                continue;

            if (window_func.aggregate_function->hasUserDefinedEmit())
                has_user_defined_emit_strategy = true;

            if (window_func.aggregate_function->udfType() == UDFType::Javascript)
                query_info.has_javascript_uda = true;
        }
    }

    /// UDA with own emit strategy only support stream query
    if (!isStreamingQuery() && has_user_defined_emit_strategy)
        throw Exception(
            ErrorCodes::UDA_NOT_APPLICABLE, "User Defined Aggregate function with own emit strategy cannot be used in non-streaming query");
}

/// Preliminary LIMIT - is used in every source, if there are several sources, before they are combined.
void InterpreterSelectQuery::executeStreamingPreLimit(QueryPlan & query_plan, bool do_not_skip_offset)
{
    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
    {
        auto [limit_length, limit_offset] = getLimitLengthAndOffset(query, context);

        if (do_not_skip_offset)
        {
            if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
                return;

            limit_length += limit_offset;
            limit_offset = 0;
        }

        auto limit = std::make_unique<Streaming::LimitStep>(query_plan.getCurrentDataStream(), limit_length, limit_offset);
        if (do_not_skip_offset)
            limit->setStepDescription("preliminary Streaming LIMIT (with OFFSET)");
        else
            limit->setStepDescription("preliminary Streaming LIMIT (without OFFSET)");

        query_plan.addStep(std::move(limit));
    }
}

void InterpreterSelectQuery::executeStreamingLimit(QueryPlan & query_plan)
{
    auto & query = getSelectQuery();
    /// If there is LIMIT
    if (query.limitLength())
    {
        /** Rare case:
          *  if there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
          *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
          *  because if you cancel the query, we will not get `totals` data from the remote server.
          *
          * Another case:
          *  if there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
          *  otherwise TOTALS is counted according to incomplete data.
          */
        bool always_read_till_end = false;

        if (query.group_by_with_totals && !query.orderBy())
            always_read_till_end = true;

        if (!query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(query))
            always_read_till_end = true;

        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, context);

        SortDescription order_descr;
        if (query.limit_with_ties)
        {
            if (!query.orderBy())
                throw Exception("Streaming LIMIT WITH TIES without ORDER BY", ErrorCodes::LOGICAL_ERROR);
            order_descr = getSortDescription(query, context);
        }

        auto limit = std::make_unique<Streaming::LimitStep>(
                query_plan.getCurrentDataStream(),
                limit_length, limit_offset, always_read_till_end, query.limit_with_ties, order_descr);

        if (query.limit_with_ties)
            limit->setStepDescription("Streaming LIMIT WITH TIES");

        query_plan.addStep(std::move(limit));
    }
}

void InterpreterSelectQuery::executeStreamingOffset(QueryPlan & query_plan)
{
    auto & query = getSelectQuery();
    /// If there is not a LIMIT but an offset
    if (!query.limitLength() && query.limitOffset())
    {
        UInt64 limit_length;
        UInt64 limit_offset;
        std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, context);

        auto offsets_step = std::make_unique<Streaming::OffsetStep>(query_plan.getCurrentDataStream(), limit_offset);
        query_plan.addStep(std::move(offsets_step));
    }
}
/// proton: ends
}
