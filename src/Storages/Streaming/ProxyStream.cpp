#include "ProxyStream.h"
#include "StorageStream.h"

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Streaming/DedupTransformStep.h>
#include <Processors/QueryPlan/Streaming/SessionStep.h>
#include <Processors/QueryPlan/Streaming/SessionStepWithSubstream.h>
#include <Processors/QueryPlan/Streaming/TimestampTransformStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStepWithSubstream.h>
#include <Processors/QueryPlan/Streaming/WindowAssignmentStep.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageRandom.h>
#include <Common/ProtonCommon.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace Streaming
{
namespace
{
ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: no stream expression in view select AST");

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: incorrect stream expression");

    return select_element->table_expression->as<ASTTableExpression>();
}

ContextMutablePtr createProxySubqueryContext(const ContextPtr & context, const SelectQueryInfo & query_info, bool is_streaming)
{
    auto sub_context = Context::createCopy(context);

    /// In case: `with cte as (select *, _tp_sn as sn from stream) select count() from tumble(cte, 2s) where sn >= 0 group by window_start`
    /// There are two scenarios:
    /// 1) After predicates push down optimized, actual cte is `select *, _tp_sn as sn from stream where sn >= 0 settings seek_to=0`
    /// 2) Otherwise, actual cte is `select *, _tp_sn as sn from stream settings seek_to=0`
    /// NOTE: No need settings `seek_to`, the event predicates should always can be push down (e.g. `select *, _tp_sn as sn from stream where sn >= 0`)
    // if (query_info.seek_to_info)
    //     sub_context->applySettingChange({"seek_to", query_info.seek_to_info->checkAndGetSeekToForSettings()});

    /// In case: `with cte as (select *, _tp_sn as sn from stream) select count() from table(cte)`
    /// actual cte is `select *, _tp_sn as sn from stream settings query_mode='table'`
    if (!is_streaming)
        sub_context->applySettingChange({"query_mode", "table"});

    return sub_context;
}
}

ProxyStream::ProxyStream(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    ContextPtr context_,
    FunctionDescriptionPtr streaming_func_desc_,
    FunctionDescriptionPtr timestamp_func_desc_,
    StoragePtr nested_proxy_storage_,
    String internal_name_,
    ASTPtr subquery_,
    bool streaming_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , streaming_func_desc(std::move(streaming_func_desc_))
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , nested_proxy_storage(nested_proxy_storage_)
    , internal_name(std::move(internal_name_))
    , subquery(subquery_)
    , streaming(streaming_)
    , log(&Poco::Logger::get(id_.getNameForLogs()))
{
    if (streaming)
        assert(streaming_func_desc);

    if (nested_proxy_storage)
        assert(nested_proxy_storage->as<ProxyStream>());

    validateProxyChain();

    if (windowType() == WindowType::SESSION && !isStreaming())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "session window can only work with streaming query.");

    if (!subquery)
    {
        storage = DatabaseCatalog::instance().getTable(id_, context_);
    }
    else
    {
        /// Whether has GlobalAggregation in subquery
        SelectQueryOptions options;
        auto interpreter_subquery
            = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context_, options.subquery().analyze());
        has_global_aggr = interpreter_subquery->hasGlobalAggregation();
    }

    StorageInMemoryMetadata storage_metadata = storage ? storage->getInMemoryMetadata() : StorageInMemoryMetadata();
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

QueryProcessingStage::Enum ProxyStream::getQueryProcessingStage(
    ContextPtr context_,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    if (storage)
        return storage->getQueryProcessingStage(context_, to_stage, storage_snapshot, query_info);
    else
        /// When it is created by subquery not a table
        return QueryProcessingStage::FetchColumns;
}

void ProxyStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    /// issue-1289
    /// If current table function is session / tumble / hop,
    /// we need drop STREAMING_WINDOW_START/END columns before forwarding the request
    /// since for these table functions, we will add window_start/end automatically
    /// Event the inner is a table function or stream or view or MV which contains window_start/end,
    /// we choose the outer table function's window_start/end override the inner ones.
    Names updated_column_names;
    updated_column_names.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        if (windowType() != WindowType::NONE
            && (column_name == ProtonConsts::STREAMING_WINDOW_START || column_name == ProtonConsts::STREAMING_WINDOW_END
                || column_name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS || column_name == ProtonConsts::STREAMING_SESSION_START
                || column_name == ProtonConsts::STREAMING_SESSION_END))
            continue;

        updated_column_names.push_back(column_name);
    }

    /// If this storage is built from subquery
    assert(!(storage && subquery));

    if (query_info.proxy_stream_query)
    {
        if (!query_info.proxy_stream_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized ProxyStream query");

        auto sub_context = createProxySubqueryContext(context_, query_info, isStreaming());
        auto interpreter = std::make_unique<InterpreterSelectWithUnionQuery>(
            query_info.proxy_stream_query->clone(), sub_context, SelectQueryOptions().subquery().noModify(), updated_column_names);

        interpreter->ignoreWithTotals();
        interpreter->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(sub_context);
        return;
    }

    if (subquery)
    {
        auto sub_context = createProxySubqueryContext(context_, query_info, isStreaming());
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], sub_context, SelectQueryOptions().subquery().noModify(), updated_column_names);

        interpreter_subquery->ignoreWithTotals();
        interpreter_subquery->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(sub_context);
        return;
    }

    if (auto * view = storage->as<StorageView>())
    {
        auto view_context = createProxySubqueryContext(context_, query_info, isStreaming());
        view->read(
            query_plan, updated_column_names, storage_snapshot, query_info, view_context, processed_stage, max_block_size, num_streams);
        query_plan.addInterpreterContext(view_context);
        return;
    }
    else if (auto * materialized_view = storage->as<StorageMaterializedView>())
        return materialized_view->read(
            query_plan, updated_column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (auto * external_stream = storage->as<StorageExternalStream>())
        return external_stream->read(
            query_plan, updated_column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (auto * random_stream = storage->as<StorageRandom>())
        return random_stream->read(
            query_plan, updated_column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (nested_proxy_storage)
        return nested_proxy_storage->read(
            query_plan, updated_column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);

    auto * distributed = storage->as<StorageStream>();
    assert(distributed);
    distributed->read(
        query_plan, updated_column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

NamesAndTypesList ProxyStream::getVirtuals() const
{
    if (nested_proxy_storage)
        return nested_proxy_storage->getVirtuals();

    if (!storage)
        return {};

    if (auto * materialized_view = storage->as<StorageMaterializedView>(); materialized_view)
        return materialized_view->getVirtuals();

    if (auto * distributed = storage->as<StorageStream>(); distributed)
    {
        if (streaming)
            return distributed->getVirtuals();
        else
            return distributed->getVirtualsHistory();
    }

    return {};
}

Names ProxyStream::getAdditionalRequiredColumns() const
{
    Names required;

    if (nested_proxy_storage)
        required = nested_proxy_storage->as<ProxyStream>()->getAdditionalRequiredColumns();

    if (timestamp_func_desc)
        for (const auto & name : timestamp_func_desc->input_columns)
            required.push_back(name);

    if (streaming_func_desc)
        for (const auto & name : streaming_func_desc->input_columns)
            if (name != ProtonConsts::STREAMING_TIMESTAMP_ALIAS)
                /// We remove the internal dependent __tp_ts column
                required.push_back(name);

    return required;
}

void ProxyStream::validateProxyChain() const
{
    /// We only support this sequence tumble(dedup(table(...), ...), ...)
    static const std::unordered_map<String, UInt8> func_name_order_map = {
        {"table", 1},
        {"dedup", 2},
        {"tumble", 3},
        {"hop", 3},
        {"session", 3},
    };

    auto prev_name = internal_name;
    auto prev = func_name_order_map.at(prev_name);
    auto * next_proxy = nested_proxy_storage ? nested_proxy_storage->as<ProxyStream>() : nullptr;

    while (next_proxy)
    {
        auto cur = func_name_order_map.at(next_proxy->internal_name);
        if (cur >= prev)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Wrap `{}` over `{}` is not supported. Use this wrap sequence: tumble/hop/session -> dedup -> table",
                prev_name,
                next_proxy->internal_name);

        if (next_proxy->nested_proxy_storage)
            next_proxy = next_proxy->nested_proxy_storage->as<ProxyStream>();
        else
            next_proxy = nullptr;
    }
}

void ProxyStream::buildStreamingProcessingQueryPlan(
    QueryPlan & query_plan,
    const Names & required_columns_after_streaming_window,
    const SelectQueryInfo & query_info,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context_,
    bool need_watermark,
    size_t curr_proxy_depth) const
{
    if (nested_proxy_storage)
        nested_proxy_storage->as<ProxyStream>()->buildStreamingProcessingQueryPlan(
            query_plan, required_columns_after_streaming_window, query_info, storage_snapshot, context_, need_watermark, ++curr_proxy_depth);

    if (!streaming_func_desc)
        return;

    /// tumble(dedup(table(stream), columns...), 5s)
    if (internal_name == "dedup")
    {
        /// Insert dedup step
        query_plan.addStep(std::make_unique<DedupTransformStep>(
            query_plan.getCurrentDataStream(), query_plan.getCurrentDataStream().header, streaming_func_desc));

        /// In case `select count() from dedup(stream, s)`, no streamign window, but still need watermark
        if (need_watermark && curr_proxy_depth == 1)
            processWatermarkStep(query_plan, query_info, false);
    }
    else if (streaming_func_desc->type != WindowType::NONE)
    {
        auto proc_time = processTimestampStep(query_plan, required_columns_after_streaming_window, context_);

        if (need_watermark)
            processWatermarkStep(query_plan, query_info, proc_time);

        processWindowAssignmentStep(query_plan, required_columns_after_streaming_window, storage_snapshot);
    }
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} function is not supported", internal_name);
}

bool ProxyStream::processTimestampStep(
    QueryPlan & query_plan, const Names & required_columns_after_streaming_window, const ContextPtr & context_) const
{
    bool proc_time = false;
    if (!timestamp_func_desc)
        return proc_time;

    proc_time = timestamp_func_desc->is_now_func;
    auto output_header = query_plan.getCurrentDataStream().header;
    /// Drop timestamp expr required columns if they are not required by downstream pipe
    auto required_begin = required_columns_after_streaming_window.begin();
    auto required_end = required_columns_after_streaming_window.end();
    for (const auto & name : timestamp_func_desc->input_columns)
        if (std::find(required_begin, required_end, name) == required_end)
            output_header.erase(name);

    /// Add transformed timestamp column required by downstream pipe
    auto timestamp_col = timestamp_func_desc->expr->getSampleBlock().getByPosition(0);
    assert(!output_header.findByName(timestamp_col.name));
    timestamp_col.column = timestamp_col.type->createColumnConstWithDefaultValue(0);
    output_header.insert(timestamp_col);

    const auto & seek_to = context_->getSettingsRef().seek_to.value;
    bool backfill = !seek_to.empty() && seek_to != "latest" && seek_to != "earliest";

    query_plan.addStep(
        std::make_unique<TimestampTransformStep>(query_plan.getCurrentDataStream(), output_header, timestamp_func_desc, backfill));

    return proc_time;
}

void ProxyStream::processWatermarkStep(QueryPlan & query_plan, const SelectQueryInfo & query_info, bool proc_time) const
{
    if (streaming_func_desc && streaming_func_desc->type == WindowType::SESSION)
    {
        processSessionStep(query_plan, query_info);
    }
    else
    {
        Block output_header = query_plan.getCurrentDataStream().header.cloneEmpty();
        if (query_info.hasPartitionByKeys())
            query_plan.addStep(std::make_unique<Streaming::WatermarkStepWithSubstream>(
                query_plan.getCurrentDataStream(),
                std::move(output_header),
                query_info.query,
                query_info.syntax_analyzer_result,
                streaming_func_desc,
                proc_time,
                log));
        else
            query_plan.addStep(std::make_unique<Streaming::WatermarkStep>(
                query_plan.getCurrentDataStream(),
                std::move(output_header),
                query_info.query,
                query_info.syntax_analyzer_result,
                streaming_func_desc,
                proc_time,
                log));
    }
}

void ProxyStream::processSessionStep(QueryPlan & query_plan, const SelectQueryInfo & query_info) const
{
    assert(streaming_func_desc->type == WindowType::SESSION);

    Block output_header = query_plan.getCurrentDataStream().header.cloneEmpty();

    /// Same FIXME as in InterpreterSelectQuery::buildStreamingProcessingQueryPlanForSessionWindow
    size_t insert_pos = 0;
    auto session_start_type = std::make_shared<DataTypeUInt8>();
    output_header.insert(insert_pos++, {session_start_type, ProtonConsts::STREAMING_SESSION_START});

    auto session_end_type = std::make_shared<DataTypeUInt8>();
    output_header.insert(insert_pos++, {session_end_type, ProtonConsts::STREAMING_SESSION_END});

    if (query_info.hasPartitionByKeys())
        query_plan.addStep(std::make_unique<Streaming::SessionStepWithSubstream>(
            query_plan.getCurrentDataStream(), std::move(output_header), streaming_func_desc));
    else
        query_plan.addStep(
            std::make_unique<Streaming::SessionStep>(query_plan.getCurrentDataStream(), std::move(output_header), streaming_func_desc));
}

void ProxyStream::processWindowAssignmentStep(
    QueryPlan & query_plan, const Names & required_columns_after_streaming_window, const StorageSnapshotPtr & storage_snapshot) const
{
    if (streaming_func_desc->type == WindowType::TUMBLE || streaming_func_desc->type == WindowType::HOP
        || streaming_func_desc->type == WindowType::SESSION)
    {
        Block output_header = storage_snapshot->getSampleBlockForColumns(required_columns_after_streaming_window);

        query_plan.addStep(
            std::make_unique<WindowAssignmentStep>(query_plan.getCurrentDataStream(), std::move(output_header), streaming_func_desc));
    }
}

StorageSnapshotPtr ProxyStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    if (auto nested = getNestedStorage())
        return nested->getStorageSnapshot(metadata_snapshot, std::move(query_context));

    return IStorage::getStorageSnapshot(metadata_snapshot, std::move(query_context));
}

bool ProxyStream::isRemote() const
{
    if (auto nested = getNestedStorage())
        return nested->isRemote();

    return IStorage::isRemote();
}

bool ProxyStream::supportsParallelInsert() const
{
    if (auto nested = getNestedStorage())
        return nested->supportsParallelInsert();

    return IStorage::supportsParallelInsert();
}

bool ProxyStream::supportsIndexForIn() const
{
    if (auto nested = getNestedStorage())
        return nested->supportsIndexForIn();

    return IStorage::supportsIndexForIn();
}

bool ProxyStream::supportsSubcolumns() const
{
    if (auto nested = getNestedStorage())
        return nested->supportsSubcolumns();

    return IStorage::supportsSubcolumns();
}

StoragePtr ProxyStream::getNestedStorage() const
{
    if (nested_proxy_storage)
        return nested_proxy_storage;

    if (storage)
        return storage;

    return nullptr; /// subquery
}

bool ProxyStream::isProxyingSubqueryOrView() const
{
    if (nested_proxy_storage)
        return nested_proxy_storage->as<ProxyStream &>().isProxyingSubqueryOrView();

    if (subquery)
        return true;

    if (storage && storage->as<StorageView>())
        return true;

    return false;
}

/// For examples:
/// Assume `test_v` view as `select i, s, _tp_time from test`
/// 1) Query-1:
///     @outer_query: with cte as select i, s, _tp_time from test select sum(i) from tumble(cte, 2s)
///    Replaced:
///     @outer_query: with cte as select i, s, _tp_time from test select sum(i) from (select i, s, _tp_time from test)
///     @return (saved_proxy_stream): tumble(cte, 2s)
/// 2) Query-2:
///     @outer_query: select i, s, _tp_time from table(test_v)
///    Replaced:
///     @outer_query: select i from (select i, s, _tp_time from test)
///     @return (saved_proxy_stream): table(test_v)
ASTPtr ProxyStream::replaceWithSubquery(ASTSelectQuery & outer_query) const
{
    assert(isProxyingSubqueryOrView());

    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression || !table_expression->table_function)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: incorrect stream expression");

    DatabaseAndTableWithAlias db_table(*table_expression);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    ASTPtr saved_proxy_stream = table_expression->table_function;
    table_expression->table_function = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->setAlias(alias);
    if (subquery)
        table_expression->subquery->children.push_back(subquery->children[0]->clone());
    else
        table_expression->subquery->children.push_back(getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone());

    for (auto & child : table_expression->children)
        if (child.get() == saved_proxy_stream.get())
            child = table_expression->subquery;

    return saved_proxy_stream;
}

/// For examples (follow the above `replaceWithSubquery`):
/// 1) Query-1:
///     @outer_query (optimized): with cte as select i, s, _tp_time from test select sum(i) from (select i from test)
///     @saved_proxy_stream: tumble(cte, 2s)
///    Restored:
///     @outer_query: with cte as select i, s, _tp_time from test select sum(i) from tumble(cte, 2s)
///     @return (optimized_proxy_stream_query): (select i from test)
///     @
/// 2) Query-2:
///     @outer_query: select i from (select i from test)
///     @saved_proxy_stream: table(test_v)
///    Restored:
///     @outer_query: select i from table(test_v)
///     @return (optimized_proxy_stream_query): (select i from test)
ASTPtr ProxyStream::restoreProxyStreamName(ASTSelectQuery & outer_query, const ASTPtr & saved_proxy_stream) const
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->subquery)
        throw Exception("Logical error: incorrect stream expression", ErrorCodes::LOGICAL_ERROR);

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->table_function = saved_proxy_stream;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = saved_proxy_stream;
    return subquery->children[0];
}
}
}
