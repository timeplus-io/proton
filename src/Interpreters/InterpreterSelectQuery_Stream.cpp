#include <Interpreters/InterpreterSelectQuery.h>

#include <DataTypes/ObjectUtils.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>
#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Interpreters/Streaming/ChangelogQueryVisitor.h>
#include <Interpreters/Streaming/EmitInterpreter.h>
#include <Interpreters/Streaming/EventPredicateVisitor.h>
#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Interpreters/Streaming/SubstituteStreamingFunction.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Processors/QueryPlan/LightShufflingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Streaming/AggregatingStep.h>
#include <Processors/QueryPlan/Streaming/AggregatingStepWithSubstream.h>
#include <Processors/QueryPlan/Streaming/LimitStep.h>
#include <Processors/QueryPlan/Streaming/OffsetStep.h>
#include <Processors/QueryPlan/Streaming/SortingStep.h>
#include <Processors/QueryPlan/Streaming/SubstreamShufflingStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStepWithSubstream.h>
#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/Streaming/EmitParams.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Proxy/ProxyStream.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageView.h>
#include <Storages/Stream/storageUtil.h>
#include <Common/ProtonCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int UNSUPPORTED;
extern const int UDA_NOT_APPLICABLE;
extern const int WINDOW_COLUMN_NOT_REFERENCED;
}

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

std::optional<Streaming::EmitAfterKeyExpirationParams>
getEmitAfterKeyExpirationParams(const QueryPlan & query_plan, Streaming::EmitParamsPtr emit_params_copy, HashTableType hash_table_type)
{
    if (emit_params_copy->mode != Streaming::EmitMode::AfterKeyExpire)
        return {};

    if (hash_table_type != HashTableType::Hybrid)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "`EMIT AFTER KEY EXPIRE` aggregation only supports hybrid hash table, use `SETTINGS default_hash_table='hybrid' for the "
            "query");

    std::optional<size_t> key_ts_col_pos;
    const auto & header = query_plan.getCurrentDataStream().header;
    if (!emit_params_copy->key_ts_col_name.empty())
    {
        key_ts_col_pos = header.tryGetPositionByName(emit_params_copy->key_ts_col_name);
        if (!key_ts_col_pos)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Timestamp column='{}' clause is missing which is required by `EMIT AFTER KEY EXPIRE IDENTIFIED BY {}`",
                emit_params_copy->key_ts_col_name,
                emit_params_copy->key_ts_col_name);
    }
    else
    {
        /// Try _tp_time
        key_ts_col_pos = header.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_TIME);
        if (!key_ts_col_pos)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "'{}' column is missing which is required by `EMIT AFTER KEY EXPIRE` clause when `IDENTIFIED BY ts_col` is not present",
                ProtonConsts::RESERVED_EVENT_TIME);
    }

    auto [max_span_ms, timeout_ms] = emit_params_copy->keyMaxSpanAndTimeoutMs();

    /// Check date type of key_ts_col
    const auto & ts_col = header.getByPosition(key_ts_col_pos.value());

    /// Convert the scale / unit to the same as ts column
    UInt64 max_span = max_span_ms;
    auto col_type = ts_col.type->getTypeId();
    if (col_type == TypeIndex::DateTime)
    {
        /// Seconds precision
        if (max_span_ms % 1000 != 0)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "`IDENTIFIED BY ts_col` is datetime which has seconds precision but the `MAXSPAN` is not full in seconds. Pick a "
                "MAXSPAN which is full in seconds. For example, `2s`");

        max_span = max_span_ms / 1000;
    }
    else if (col_type == TypeIndex::DateTime64)
    {
        auto scale_multiplier = static_cast<const DataTypeDateTime64 *>(ts_col.type.get())->getScaleMultiplier();
        if (scale_multiplier < 1000)
            max_span = (max_span_ms / 1000) * scale_multiplier;
        else if (scale_multiplier > 1000)
            max_span = max_span_ms * (scale_multiplier / 1000);
    }
    else
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED, "`IDENTIFIED BY ts_col` only supports DateTime or DateTime64 types, but got '{}'", col_type);
    }

    return Streaming::EmitAfterKeyExpirationParams{
        .key_ts_col_pos = key_ts_col_pos.value(),
        .key_max_span_interval = max_span,
        .timeout_interval_ms = timeout_ms,
        .only_max_span = emit_params_copy->only_max_span,
    };
}

/// proton: starts. Add a new parameter `join_result_emit_changelog`
void rewriteMultipleJoins(
    ASTPtr & query, const TablesWithColumns & tables, const String & database, const Settings & settings, bool join_result_emit_changelog)
{
    ASTSelectQuery & select = query->as<ASTSelectQuery &>();

    Aliases aliases;
    if (ASTPtr with = select.with())
        QueryAliasesNoSubqueriesVisitor(aliases).visit(with);
    QueryAliasesNoSubqueriesVisitor(aliases).visit(select.select());

    CrossToInnerJoinVisitor::Data cross_to_inner{tables, aliases, database};
    cross_to_inner.cross_to_inner_join_rewrite = static_cast<UInt8>(std::min<UInt64>(settings.cross_to_inner_join_rewrite, 2));
    CrossToInnerJoinVisitor(cross_to_inner).visit(query);

    JoinToSubqueryTransformVisitor::Data join_to_subs_data{tables, aliases, join_result_emit_changelog};
    JoinToSubqueryTransformVisitor(join_to_subs_data).visit(query);
}
/// proton: ends.

}

void InterpreterSelectQuery::processEmits()
{
    if (auto emit = getSelectQuery().emit())
    {
        const Settings & settings = context->getSettingsRef();

        Streaming::EmitInterpreter::handleRules(
            query_ptr, Streaming::EmitInterpreter::checkEmitAST, Streaming::EmitInterpreter::LastXRule(settings, log));

        /// Force emit changelog, for example: `select * from versioned_kv emit changelog`
        if (emit->as<ASTEmitQuery &>().stream_mode.value_or(ASTEmitQuery::StreamMode::STREAM) == ASTEmitQuery::StreamMode::CHANGELOG)
            query_info.force_emit_changelog = true;

        /// After handling, update setting for context.
        if (getSelectQuery().settings())
            InterpreterSetQuery(getSelectQuery().settings(), context).executeForCurrentContext();
    }
}

void InterpreterSelectQuery::clearInits()
{
    storage = nullptr;
    table_lock.reset();
    table_id = StorageID::createEmpty();
    metadata_snapshot = nullptr;
    storage_snapshot = nullptr;
}

bool InterpreterSelectQuery::resolveTablesAndRewriteJoin(JoinedTables & joined_tables)
{
    const bool has_input = input_pipe.has_value();
    const Settings & settings = context->getSettingsRef();

    bool got_storage_from_query = false;
    if (!has_input && !storage)
    {
        /// proton: starts.
        /// we will translate storage to ProxyStream for `table(name)`
        /// proton: ends.
        storage = joined_tables.getLeftTableStorage();
        // Mark uses_view_source if the returned storage is the same as the one saved in viewSource
        uses_view_source |= storage && storage == context->getViewSource();
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
            else if (auto * mv = storage->as<StorageMaterializedView>())
            {
                metadata_snapshot = mv->getTargetInMemoryMetadataPtr();
            }
            else
            {
                metadata_snapshot = storage->getInMemoryMetadataPtr();
            }
        }
        /// proton: ends

        if (options.only_analyze)
            storage_snapshot = storage->getStorageSnapshotWithoutData(metadata_snapshot, context);
        else
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

    /// FIXME: Memory bound aggregation may cause another reading algorithm to be used on remote replicas
    if (settings.allow_experimental_parallel_reading_from_replicas && settings.enable_memory_bound_merging_of_aggregation_results)
        context->setSetting("enable_memory_bound_merging_of_aggregation_results", false);

    if (joined_tables.tablesCount() > 1
        && (!settings.parallel_replicas_custom_key.value.empty() || settings.allow_experimental_parallel_reading_from_replicas))
    {
        LOG_WARNING(log, "Joins are not supported with parallel replicas. Query will be executed without using them.");
        context->setSetting("allow_experimental_parallel_reading_from_replicas", false);
        context->setSetting("parallel_replicas_custom_key", String{""});
    }

    /// Rewrite JOINs
    if (!has_input && joined_tables.tablesCount() > 1)
    {
        /// proton: starts.
        rewriteMultipleJoins(
            query_ptr,
            joined_tables.tablesWithColumns(),
            context->getCurrentDatabase(),
            context->getSettingsRef(),
            /*join_result_emit_changelog=*/query_info.force_emit_changelog);
        /// proton: ends.

        joined_tables.reset(getSelectQuery());
        joined_tables.resolveTables();
        if (auto view_source = context->getViewSource())
        {
            // If we are using a virtual block view to replace a table and that table is used
            // inside the JOIN then we need to update uses_view_source accordingly so we avoid propagating scalars that we can't cache
            const auto & storage_values = static_cast<const StorageValues &>(*view_source);
            auto tmp_table_id = storage_values.getStorageID();
            for (const auto & t : joined_tables.tablesWithColumns())
                uses_view_source |= (t.table.database == tmp_table_id.database_name && t.table.table == tmp_table_id.table_name);
        }

        if (storage && joined_tables.isLeftTableSubquery())
        {
            /// Rewritten with subquery. Free storage locks here.
            clearInits();
        }
    }

    if (!has_input)
    {
        interpreter_subquery = joined_tables.makeLeftTableSubquery(options.subquery());
        if (interpreter_subquery)
        {
            source_header = interpreter_subquery->getSampleBlock();
            uses_view_source |= interpreter_subquery->usesViewSource();
        }
    }

    /// proton : starts. After resolving the tables and rewrite multiple joins
    /// we will have at most 2 tables : left table (or subquery) and right table (or subquery)
    /// It is a good time to resolve the data stream semantic of the whole query
    resolveDataStreamSemantic(joined_tables);
    /// proton : ends

    return got_storage_from_query;
}

bool InterpreterSelectQuery::analyzeRequiredColumns(
    const Names & required_result_column_names, JoinedTables & joined_tables, std::unique_ptr<Names> & new_required_result_column_names)
{
    bool got_storage_from_query = false;

    const auto & tables = joined_tables.tablesWithColumns();
    assert(tables.size() <= 2);

    /// An optimized case: global aggr over global aggr, for example:
    /// `select sum(cnt) from (select id, count() as cnt from stream group by id) where cnt > 1`
    /// Force the nested global aggr emit changelog and the outer global aggr still keep aggregated states.
    bool force_single_subquery_input_to_emit_changelog = false;
    if (tables.size() == 1 && hasGlobalAggregationInQuery(query_ptr, getSelectQuery(), storage))
    {
        if (interpreter_subquery && interpreter_subquery->hasStreamingGlobalAggregation())
        {
            force_single_subquery_input_to_emit_changelog = true;
        }
        else if (storage)
        {
            if (const auto * proxy = storage->as<Streaming::ProxyStream>(); proxy && proxy->hasStreamingGlobalAggregation())
                force_single_subquery_input_to_emit_changelog = true;
            else if (const auto * view = storage->as<StorageView>(); view && view->hasStreamingGlobalAggregation())
                force_single_subquery_input_to_emit_changelog = true;
        }

        if (force_single_subquery_input_to_emit_changelog)
        {
            query_info.left_input_tracking_changes = true;
            data_stream_semantic_pair.effective_input_data_stream_semantic = Streaming::DataStreamSemantic::Changelog;
        }
    }

    if (isStreamingQuery()
        && (query_info.trackingChanges() || data_stream_semantic_pair.isChangelogOutput() || force_single_subquery_input_to_emit_changelog))
    {
        /// Rewrite select query to add back _tp_delta if it is not present
        Streaming::ChangelogQueryVisitorMatcher data(
            data_stream_semantic_pair, tables, !required_result_column_names.empty(), options.is_subquery, query_info);

        Streaming::ChangelogQueryVisitor(data).visit(query_ptr);
        if (data.queryInputIsRewritten())
        {
            clearInits();

            joined_tables.reset(getSelectQuery());
            got_storage_from_query = resolveTablesAndRewriteJoin(joined_tables);
        }

        if (auto && new_required_columns = data.newRequiredResultColumnNames(); !new_required_columns.empty())
        {
            /// Make a copy of existing required result column names and add the new ones
            new_required_result_column_names = std::make_unique<Names>(required_result_column_names);
            for (auto & new_required_column : new_required_columns)
            {
                if (std::find(new_required_result_column_names->begin(), new_required_result_column_names->end(), new_required_column)
                    == new_required_result_column_names->end())
                    new_required_result_column_names->emplace_back(std::move(new_required_column));
            }
        }
    }

    return got_storage_from_query;
}

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
        context->getTempDataOnDisk(),
        settings.min_free_disk_space_for_temporary_data);

    sorting_step->setStepDescription("Streaming Sorting for ORDER BY");
    query_plan.addStep(std::move(sorting_step));
}

void InterpreterSelectQuery::executeStreamingAggregation(
    QueryPlan & query_plan, const ActionsDAGPtr & expression, bool overflow_row, bool final)
{
    assert(isStreamingQuery());

    if (!final)
        throw Exception(ErrorCodes::UNSUPPORTED, "Distributed merge is not implemented for streaming query");

    if (overflow_row)
        throw Exception(ErrorCodes::UNSUPPORTED, "Overflow aggregation is not implemented for streaming query");

    executeExpression(query_plan, expression, "Before GROUP BY");

    if (options.is_projection_query)
        return;

    auto streaming_group_by = Streaming::IAggregatorParams::GroupBy::Other;

    const auto & header_before_aggregation = query_plan.getCurrentDataStream().header;

    Names keys;

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
                keys.insert(keys.begin(), key.name);
                streaming_group_by = Streaming::IAggregatorParams::GroupBy::WindowEnd;
                ++window_keys_num;
                continue;
            }
            else if (
                (key.name == ProtonConsts::STREAMING_WINDOW_START) && (isDate(key.type) || isDateTime(key.type) || isDateTime64(key.type)))
            {
                keys.insert(keys.begin(), key.name);
                streaming_group_by = Streaming::IAggregatorParams::GroupBy::WindowStart;
                ++window_keys_num;
                continue;
            }
        }

        keys.push_back(key.name);
    }

    const auto & aggregates = query_analyzer->aggregates();
    if (has_user_defined_emit_strategy)
    {
        if (aggregates.size() > 1)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "User defined aggregation function with emit strategy shouldn't be used together with other aggregation function");

        if (windowType() != Streaming::WindowType::None)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "User defined aggregation function with emit strategy shouldn't be used together with streaming window");

        assert(streaming_group_by == Streaming::IAggregatorParams::GroupBy::Other);
        streaming_group_by = Streaming::IAggregatorParams::GroupBy::UserDefined;
    }

    const Settings & settings = context->getSettingsRef();

    /// TODO: support more overflow mode
    if (unlikely(settings.group_by_overflow_mode != OverflowMode::THROW))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Streaming aggregation group by overflow mode '{}' is not implemented",
            magic_enum::enum_name(settings.group_by_overflow_mode.value));

    auto emit_params_copy = emitParams();
    auto tracking_updates_type = Streaming::TrackingUpdatesType::None;
    if (data_stream_semantic_pair.isChangelogOutput())
        tracking_updates_type = Streaming::TrackingUpdatesType::UpdatesWithRetract;
    /// TODO: A optimization for `emit on update`, we don't need to track updates and just directly convert each input (fast in and fast out)
    else if (Streaming::AggregatingHelper::onlyEmitUpdates(emit_params_copy->mode))
        tracking_updates_type = Streaming::TrackingUpdatesType::Updates;

    Streaming::IAggregatorParamsPtr params;

    switch (settings.default_hash_table.value)
    {
        case HashTableType::Memory:
        {
            params = std::make_shared<Streaming::MemoryAggregatorParams>(
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
                context->getTempDataOnDisk(),
                settings.max_threads,
                settings.min_free_disk_space_for_temporary_data,
                settings.compile_aggregate_expressions,
                settings.min_count_to_compile_aggregate_expression,
                settings.max_block_size,
                settings.keep_windows,
                streaming_group_by,
                delta_col_pos,
                window_keys_num,
                query_info.streaming_window_params,
                tracking_updates_type,
                settings.optimize_aggregation_emit_on_updates);

            break;
        }
        case HashTableType::Hybrid:
        {
            auto emit_key_params = getEmitAfterKeyExpirationParams(query_plan, emit_params_copy, settings.default_hash_table.value);

            params = std::make_shared<Streaming::HybridAggregatorParams>(
                keys,
                aggregates,
                context->getSpillDirForCurrentQuery("aggr"),
                settings.max_hot_keys,
                settings.max_block_size,
                settings.max_threads,
                settings.compile_aggregate_expressions,
                settings.min_count_to_compile_aggregate_expression,
                streaming_group_by,
                delta_col_pos,
                tracking_updates_type,
                settings.optimize_aggregation_emit_on_updates,
                settings.keep_windows,
                window_keys_num,
                query_info.streaming_window_params,
                emit_key_params);
            break;
        }
    }

    if (query_plan.getCurrentDataStream().with_substream)
        query_plan.addStep(std::make_unique<Streaming::AggregatingStepWithSubstream>(
            query_plan.getCurrentDataStream(),
            std::move(params),
            emit_params_copy,
            emit_version,
            data_stream_semantic_pair.isChangelogOutput(),
            settings.aggregation_backfill_key_unique.value));
    else
        query_plan.addStep(std::make_unique<Streaming::AggregatingStep>(
            query_plan.getCurrentDataStream(),
            std::move(params),
            emit_params_copy,
            max_streams,
            emit_version,
            data_stream_semantic_pair.isChangelogOutput(),
            settings.keys_already_sharded.value || light_shuffled,
            settings.aggregation_backfill_key_unique.value));
}

/// Resolve input / output data stream semantic.
/// Output data stream semantic depends on the current layer of query (its inputs) as well as the parent's SELECT query
/// Basically parent SELECT pushes `has_aggregates / has_join` down to subquery, and subquery then decides its
/// output semantic according its SELECT and the data inputs
void InterpreterSelectQuery::resolveDataStreamSemantic(const JoinedTables & joined_tables)
{
    if (!isStreamingQuery() || context->getSettingsRef().enforce_append_only.value)
    {
        /// Default append
        data_stream_semantic_pair.output_data_stream_semantic.streaming = isStreamingQuery();
        return;
    }

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

void InterpreterSelectQuery::assertNoNonDeterministicFunctions(const Names & required, std::string_view msg_prefix) const
{
    auto required_actions = query_analyzer->simpleSelectActions();
    if (!required.empty())
    {
        Aliases aliases;
        QueryAliasesNoSubqueriesVisitor(aliases).visit(query_ptr->as<const ASTSelectQuery &>().select());
        NamesWithAliases name_with_aliases;
        name_with_aliases.reserve(aliases.size());
        for (auto & [alias, ast] : aliases)
            name_with_aliases.emplace_back(NameWithAlias{ast->getColumnName(), std::move(alias)});

        required_actions->addAliases(name_with_aliases);
        auto required_nodes = required_actions->findInOutputs(required);
        required_actions = ActionsDAG::cloneSubDAG(required_nodes, /*remove_aliases=*/true);
    }

    std::vector<String> non_deterministic_descs;
    for (const auto & node : required_actions->getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && !node.function_base->isDeterministicInScopeOfQuery())
            non_deterministic_descs.emplace_back(fmt::format("`{}`", node.result_name));
    }

    if (!non_deterministic_descs.empty())
        throw Exception(
            ErrorCodes::UNSUPPORTED,
            "{} does not support non-deterministic functions in the SELECT clause. "
            "Please remove or replace the following non-deterministic functions: {}",
            msg_prefix,
            fmt::join(non_deterministic_descs, ", "));
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

    return Streaming::WindowType::None;
}

bool InterpreterSelectQuery::hasStreamingGlobalAggregation() const
{
    return isStreamingQuery() && hasAggregation() && !hasStreamingWindowFunc();
}

void InterpreterSelectQuery::finalCheckAndOptimizeForStreamingQuery()
{
    const auto & settings = context->getSettingsRef();
    if (isStreamingQuery())
    {
        /// TODO
        if (query_analyzer->hasWindow())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Window functions are not supported in streaming queries");

        if (query_analyzer->hasStreamingJoin() && interpreter_subquery
            && isChangelogDataStream(interpreter_subquery->getDataStreamSemantic()))
            interpreter_subquery->assertNoNonDeterministicFunctions(
                required_columns, "The joined left subquery with changelog data stream");

        /// For now, for the following scenarios, we disable backfill from historic data store
        /// 1) User select some virtual columns which is only available in streaming store, like `_tp_sn`, `_tp_index_time`
        /// 2) Seek by streaming store sequence number
        /// 3) Replaying a stream.
        /// TODO, ideally we shall check if historical data store has `_tp_sn` etc columns, if they have, we can backfill from
        /// the historical data store as well technically. This will be a future enhancement.
        if (settings.enable_backfill_from_historical_store.value)
        {
            bool has_streaming_only_virtual_columns = std::ranges::any_of(required_columns, [](const auto & name) {
                return name == ProtonConsts::RESERVED_APPEND_TIME || name == ProtonConsts::RESERVED_INGEST_TIME
                    || name == ProtonConsts::RESERVED_PROCESS_TIME;
            });
            bool seek_by_sn = !query_info.seek_to_info->getSeekTo().empty() && !query_info.seek_to_info->isTimeBased()
                && query_info.seek_to_info->getSeekTo() != "earliest";
            if (has_streaming_only_virtual_columns || seek_by_sn || settings.replay_speed > 0)
                context->setSetting("enable_backfill_from_historical_store", false);
        }

        /// Usually, we don't care whether the backfilled data is in order. Excepts:
        /// 1) User require backfill data in order via setting `force_backfill_in_order=true`
        /// 2) User need window aggr emit result during backfill (it expects that process data in ascending event time)
        /// 3) User use stateful function in the non-aggr streaming query, for example: select lag(value) from stream where _tp_time > now() - 30m
        if ((settings.emit_during_backfill.value && hasAggregation() && hasStreamingWindowFunc())
            || (!hasAggregation() && analysis_result.hasStatefulFunctions()))
            context->setSetting("force_backfill_in_order", true);

        if (settings.force_backfill_in_order.value)
            query_info.require_in_order_backfill = true;
    }
    else
    {
        /// For distributed concat query, the initiator node may send over `SELECT ..., _tp_delta FROM kv_stream EMIT CHANGELOG SETTINGS query_mode='table'`;
        /// We just ignore `EMIT CHANGELOG` in this case
        if (query_info.force_emit_changelog && !settings.remote_fetch_concat.value)
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
                ErrorCodes::WINDOW_COLUMN_NOT_REFERENCED,
                "Neither window_start nor window_end is referenced in the query, but streaming window function is used");
    }
}

void InterpreterSelectQuery::executeSubstreamShuffling(QueryPlan & query_plan, const ActionsDAGPtr & expression, const Names & keys)
{
    executeExpression(query_plan, expression, "Before PARTITION BY");

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

    auto key_positions = keyPositions(query_plan.getCurrentDataStream().header, keys);
    query_plan.addStep(std::make_unique<Streaming::SubstreamShufflingStep>(
        query_plan.getCurrentDataStream(), std::move(key_positions), shuffle_output_streams));
}

std::shared_ptr<const Streaming::EmitParams> InterpreterSelectQuery::emitParams()
{
    if (emit_params)
        return emit_params;

    emit_params
        = std::make_shared<Streaming::EmitParams>(query_info.query, query_info.syntax_analyzer_result, query_info.streaming_window_params);
    return emit_params;
}

void InterpreterSelectQuery::buildWatermarkQueryPlan(QueryPlan & query_plan)
{
    chassert(isStreamingQuery());

    auto emit_params_copy = emitParams();
    if (emit_params_copy->mode == Streaming::EmitMode::AfterKeyExpire)
        /// There is no watermark for after-key-expire-emit
        /// since each key is separately tracked
        return;

    const auto & settings = context->getSettingsRef();
    bool skip_stamping_for_backfill_data = !settings.emit_during_backfill.value;

    if (query_plan.getCurrentDataStream().with_substream)
        query_plan.addStep(std::make_unique<Streaming::WatermarkStepWithSubstream>(
            query_plan.getCurrentDataStream(),
            emit_params_copy,
            skip_stamping_for_backfill_data,
            settings.default_hash_table,
            context->getSpillDirForCurrentQuery("watermark"),
            settings.max_hot_keys));
    else
        query_plan.addStep(std::make_unique<Streaming::WatermarkStep>(
            query_plan.getCurrentDataStream(), emit_params_copy, skip_stamping_for_backfill_data));
}

void InterpreterSelectQuery::buildStreamingProcessingQueryPlanBeforeJoin(QueryPlan & query_plan)
{
    bool has_streaming_window_aggr = isStreamingQuery() && hasAggregation() && hasStreamingWindowFunc();
    if (!has_streaming_window_aggr || has_user_defined_emit_strategy)
        return;

    auto can_push_down_shuffle = [&](const Names & shuffle_columns) {
        if (!analysis_result.hasJoin())
            return true;

        bool is_stream_join_table = !typeid_cast<Streaming::IHashJoin *>(analysis_result.join.get());
        /// Check all `partition by` key columns are from left stream
        const auto & header = query_plan.getCurrentDataStream().header;
        bool only_shuffling_left_stream = std::ranges::all_of(shuffle_columns, [&](const auto & key) { return header.has(key); });

        return is_stream_join_table && only_shuffling_left_stream;
    };

    if (analysis_result.hasPartitionBy())
    {
        /// FIXME: Refactor watermark for substream
        /// Normally, we should execute `partition by` after join, but current implementation of watermark
        /// over substream depends on shuffled data.
        /// So we allow do shuffling ahead for some special cases:
        /// 1) Non-join query
        /// 2) Streaming join table, and all partition by key columns are from left stream
        if (can_push_down_shuffle(analysis_result.before_partition_by->getRequiredColumnsNames()))
        {
            executeSubstreamShuffling(query_plan, analysis_result.before_partition_by, analysis_result.partition_by_keys);
            substream_shuffled_before_join = true;
        }
        else
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The join query with partition by clause doesn't support to use '{}' window function",
                magic_enum::enum_name(query_info.streaming_window_params->type));
        }
    }
    else if (analysis_result.hasShuffleBy())
    {
        if (can_push_down_shuffle(analysis_result.before_shuffle_by->getRequiredColumnsNames()))
        {
            executeLightShuffling(query_plan, analysis_result.before_shuffle_by, analysis_result.shuffle_by_keys);
            light_shuffled = true;
        }
    }

    buildWatermarkQueryPlan(query_plan);
}

void InterpreterSelectQuery::buildStreamingProcessingQueryPlanAfterJoin(QueryPlan & query_plan)
{
    if (!isStreamingQuery() || has_user_defined_emit_strategy || !hasStreamingGlobalAggregation())
        return;

    /// An optimizing path, skip duplicate periodic watermark.
    /// But if there is join query, we must establish new periodic watermark for joined data
    /// FIXME: if nested query has specified emit policy that is not compatible with outer emit policy, we shouldn't apply this optimization, for example:
    /// 1) select sum(cnt) from (select id, count() as cnt from stream group by id); --- OK
    /// 2) select sum(cnt) from (select id, count() as cnt from stream group by id emit on update); --- OK
    /// 3) select sum(cnt) from (select id, count() as cnt from stream group by id emit periodic 2s) emit periodic 10s; --- NOT OK
    /// 4) select sum(cnt) from (select id, count() as cnt from stream group by id emit on update) emit periodic 2s; --- NOT OK
    if (!analysis_result.hasJoin())
    {
        /// CTE subquery or view (or proxyed)
        if (storage)
        {
            if (auto * proxy = storage->as<Streaming::ProxyStream>(); proxy && proxy->hasStreamingGlobalAggregation())
                return;
            else if (auto * view = storage->as<StorageView>(); view && view->hasStreamingGlobalAggregation())
                return;
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
}

void InterpreterSelectQuery::checkUDA()
{
    has_user_defined_emit_strategy = false;
    query_info.has_javascript_uda = false;
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

std::vector<int64_t> InterpreterSelectQuery::checkReplaySettingsAndGetLastSN()
{
    const Settings & settings = context->getSettingsRef();

    /// So far, only support append-only stream (or proxyed)
    StorageStream * storagestream = nullptr;
    StorageExternalStream * external_stream = nullptr;
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
    }
    else if (const auto * proxy = storage->as<Streaming::ProxyStream>())
    {
        const auto & proxyed = proxy->getProxyStorageOrSubquery();
        const auto * nested_storage = std::get_if<StoragePtr>(&proxyed);
        if (nested_storage)
            external_stream = (*nested_storage)->as<StorageExternalStream>();
    }

    external_stream = storage->as<StorageExternalStream>();
    std::vector<int64_t> Last_sns;
    if (external_stream)
    {
        auto nested_storage = external_stream->getNested();
        if (auto * Kafka = nested_storage->as<ExternalStream::Kafka>())
            Last_sns = Kafka->getLastSNs();
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Replay Stream is only support append-only stream and external stream Kafka");
    }

    if (!storagestream && !external_stream)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Replay Stream is only support append-only stream and external stream Kafka");

    const String & replay_time_col = settings.replay_time_column;

    auto name_type = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(), replay_time_col);
    if (!name_type.has_value())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Not found replay column {} in stream", replay_time_col);

    const auto & type = name_type.value().type;
    if (replay_time_col != ProtonConsts::RESERVED_APPEND_TIME && !isDateTime64(type) && !isDateTime(type))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "The setting replay_time_column must be DateTime64 or DateTim32 type, but got {}",
            type->getName());

    if (std::ranges::none_of(required_columns, [&replay_time_col](const auto & name) { return name == replay_time_col; }))
        required_columns.emplace_back(replay_time_col);

    if (std::ranges::none_of(required_columns, [](const auto & name) { return name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID; }))
        required_columns.emplace_back(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);

    if (storagestream)
        return storagestream->getLastSNs();
    else
        return Last_sns;
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Streaming LIMIT WITH TIES without ORDER BY");
            order_descr = getSortDescription(query, context);
        }

        auto limit = std::make_unique<Streaming::LimitStep>(
            query_plan.getCurrentDataStream(), limit_length, limit_offset, always_read_till_end, query.limit_with_ties, order_descr);

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

bool InterpreterSelectQuery::isConsistentWithoutCheckpoint() const
{
    /// Assumes that historical queries are always consistent.
    if (!isStreamingQuery())
        return true;

    /// Streaming queries may be inconsistent without checkpoints if any of these conditions hold:
    /// 1) If Backfill from the historical store is disabled, the streaming store may not return all data.
    if (!context->getSettingsRef().enable_backfill_from_historical_store)
        return false;

    /// 2) A subquery is used that itself does not guarantee consistency.
    if (interpreter_subquery && !interpreter_subquery->isConsistentWithoutCheckpoint())
        return false;

    StoragePtr inner_storage = storage;
    if (storage)
    {
        if (auto * proxy = storage->as<Streaming::ProxyStream>())
        {
            auto proxyed = proxy->getProxyStorageOrSubquery();
            StoragePtr proxyed_storage;
            ASTPtr proxyed_subquery;
            if (const auto * storage_ = std::get_if<StoragePtr>(&proxyed))
            {
                if (auto * view = (*storage_)->as<StorageView>())
                    proxyed_subquery = view->getInMemoryMetadataPtr()->getSelectQuery().inner_query;
                else
                    proxyed_storage = *storage_;
            }
            else if (const auto * subquery_ = std::get_if<ASTPtr>(&proxyed))
            {
                proxyed_subquery = (*subquery_)->children[0];
            }

            if (proxyed_subquery)
            {
                /// 3) The proxied subquery might not guarantee consistent results.
                Streaming::rewriteSubquery(proxyed_subquery->as<ASTSelectWithUnionQuery &>(), query_info);
                auto nested_interpreter = std::make_unique<InterpreterSelectWithUnionQuery>(
                    proxyed_subquery, context, SelectQueryOptions().subquery().analyze().noModify());
                if (!nested_interpreter->isConsistentWithoutCheckpoint())
                    return false;
            }
            else if (proxyed_storage)
                inner_storage = proxyed_storage;
        }
    }

    if (inner_storage && !isKeyedStorage(inner_storage->dataStreamSemantic()))
    {
        /// (4) For an append-only stream, consistency cannot be guaranteed if the 'seek_to' setting is not absolute.
        ///     For example, using settings like "seek_to='latest'" or "seek_to='-1h'" may lead to inconsistent result.
        if (!query_info.seek_to_info->isAbosolute())
            return false;

        /// (5) Additionally, if the storage metadata indicates any TTL (time-to-live) constraints,
        ///     the results backfill from an append-only stream may be inconsistent.
        if (inner_storage->getInMemoryMetadataPtr()->hasAnyTTL())
            return false;
    }

    /// 6) A streaming join is employed, which might not yield consistent results.
    if (query_analyzer->hasStreamingJoin())
        return false;

    /// 7) The query outputs changelog data, which cannot guarantee consistent results.
    if (!options.is_subquery && data_stream_semantic_pair.isChangelogOutput())
        return false;

    return true;
}

void InterpreterSelectQuery::executeLightShuffling(QueryPlan & query_plan, const ActionsDAGPtr & expression, const Names & keys)
{
    executeExpression(query_plan, expression, "Before SHUFFLE BY");

    const auto & settings_ref = context->getSettingsRef();
    auto key_positions = keyPositions(query_plan.getCurrentDataStream().header, keys);
    query_plan.addStep(std::make_unique<LightShufflingStep>(
        query_plan.getCurrentDataStream(),
        std::move(key_positions),
        settings_ref.substreams.value != 0 ? settings_ref.substreams.value : settings_ref.max_threads.value));
}

}
