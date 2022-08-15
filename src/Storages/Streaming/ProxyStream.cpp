#include "ProxyStream.h"
#include "StorageStream.h"

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Streaming/DedupTransformStep.h>
#include <Processors/QueryPlan/Streaming/TimestampTransformStep.h>
#include <Processors/QueryPlan/Streaming/WatermarkStep.h>
#include <Processors/QueryPlan/Streaming/WindowAssignmentStep.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Common/ProtonCommon.h>

#include <base/logger_useful.h>

namespace DB
{
namespace Streaming
{
ProxyStream::ProxyStream(
    const StorageID & id_,
    const ColumnsDescription & columns_,
    StorageSnapshotPtr underlying_storage_snapshot_,
    ContextPtr context_,
    FunctionDescriptionPtr streaming_func_desc_,
    FunctionDescriptionPtr timestamp_func_desc_,
    StoragePtr nested_proxy_storage_,
    String internal_name_,
    ASTPtr subquery_,
    bool streaming_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , underlying_storage_snapshot(underlying_storage_snapshot_)
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
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context_, options.subquery());
        if (interpreter_subquery)
            has_global_aggr = interpreter_subquery->hasGlobalAggregation();
    }

    StorageInMemoryMetadata storage_metadata;
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

Pipe ProxyStream::read(
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, underlying_storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context_), BuildQueryPipelineSettings::fromContext(context_));
}

void ProxyStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// We drop STREAMING_WINDOW_START/END columns before forwarding the request
    Names updated_column_names;
    updated_column_names.reserve(column_names.size());
    for (const auto & column_name : column_names)
    {
        if (column_name == ProtonConsts::STREAMING_WINDOW_START || column_name == ProtonConsts::STREAMING_WINDOW_END
            || column_name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS || column_name == ProtonConsts::STREAMING_SESSION_ID
            || column_name == ProtonConsts::STREAMING_SESSION_START || column_name == ProtonConsts::STREAMING_SESSION_END)
            continue;

        updated_column_names.push_back(column_name);
    }

    /// If this storage is built from subquery
    assert(!(storage && subquery));

    if (subquery)
    {
        /// TODO: should we use a copy of context, instead of that from initial query?
        /// FIXME, we are re-interpreter subquery again ?
        SelectQueryOptions options;
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], context_, options.subquery().noModify(), updated_column_names);
        if (interpreter_subquery)
        {
            interpreter_subquery->ignoreWithTotals();
            interpreter_subquery->buildQueryPlan(query_plan);
            /// query_plan.addInterpreterContext(context);
        }
        return;
    }

    if (auto * view = storage->as<StorageView>())
        return view->read(
            query_plan,
            updated_column_names,
            underlying_storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
    else if (auto * materialized_view = storage->as<StorageMaterializedView>())
        return materialized_view->read(
            query_plan,
            updated_column_names,
            underlying_storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
    else if (auto * external_stream = storage->as<StorageExternalStream>())
        return external_stream->read(
            query_plan,
            updated_column_names,
            underlying_storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
    else if (nested_proxy_storage)
        return nested_proxy_storage->read(
            query_plan,
            updated_column_names,
            underlying_storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);

    auto * distributed = storage->as<StorageStream>();
    assert(distributed);

    if (streaming && context_->getSettingsRef().query_mode.value != "table")
        distributed->readStreaming(query_plan, query_info, updated_column_names, underlying_storage_snapshot, context_);
    else
        distributed->readHistory(
            query_plan,
            updated_column_names,
            underlying_storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            num_streams);
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
    bool need_watermark) const
{
    if (nested_proxy_storage)
        nested_proxy_storage->as<ProxyStream>()->buildStreamingProcessingQueryPlan(
            query_plan, required_columns_after_streaming_window, query_info, storage_snapshot, context_, need_watermark);

    if (!streaming_func_desc)
        return;

    /// tumble(dedup(table(stream), columns...), 5s)
    if (internal_name == "dedup")
    {
        /// Insert dedup step
        query_plan.addStep(std::make_unique<DedupTransformStep>(
            query_plan.getCurrentDataStream(), query_plan.getCurrentDataStream().header, streaming_func_desc));
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

    query_plan.addStep(std::make_unique<TimestampTransformStep>(
        query_plan.getCurrentDataStream(), output_header, std::move(timestamp_func_desc), backfill));

    return proc_time;
}

void ProxyStream::processWatermarkStep(QueryPlan & query_plan, const SelectQueryInfo & query_info, bool proc_time) const
{
    assert(streaming_func_desc && (streaming_func_desc->type != WindowType::NONE));

    Block output_header = query_plan.getCurrentDataStream().header.cloneEmpty();
    if (streaming_func_desc->type == WindowType::SESSION)
    {
        /// insert _tp_session_id column for session window
        auto data_type = std::make_shared<DataTypeUInt64>();
        output_header.insert(0, {data_type, ProtonConsts::STREAMING_SESSION_ID});

        auto session_start_type = std::make_shared<DataTypeBool>();
        output_header.insert(1, {session_start_type, ProtonConsts::STREAMING_SESSION_START});

        auto session_end_type = std::make_shared<DataTypeBool>();
        output_header.insert(2, {session_end_type, ProtonConsts::STREAMING_SESSION_END});
    }

    query_plan.addStep(std::make_unique<WatermarkStep>(
        query_plan.getCurrentDataStream(),
        std::move(output_header),
        query_info.query,
        query_info.syntax_analyzer_result,
        streaming_func_desc,
        proc_time,
        log));
}

void ProxyStream::processWindowAssignmentStep(
    QueryPlan & query_plan, const Names & required_columns_after_streaming_window, const StorageSnapshotPtr & storage_snapshot) const
{
    if (streaming_func_desc->type == WindowType::TUMBLE || streaming_func_desc->type == WindowType::HOP)
    {
        Block output_header = storage_snapshot->getSampleBlockForColumns(required_columns_after_streaming_window);

        query_plan.addStep(std::make_unique<WindowAssignmentStep>(
            query_plan.getCurrentDataStream(), std::move(output_header), streaming_func_desc));
    }
}

StorageSnapshotPtr ProxyStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot) const
{
    if (nested_proxy_storage)
        return nested_proxy_storage->getStorageSnapshot(metadata_snapshot);

    if (storage)
        return storage->getStorageSnapshot(metadata_snapshot);

    return IStorage::getStorageSnapshot(metadata_snapshot);
}
}
}
