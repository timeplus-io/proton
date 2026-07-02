#include <Storages/Proxy/ProxyStream.h>
#include <Storages/Stream/StorageStream.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Interpreters/Streaming/TimestampFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Streaming/ChangelogConvertStep.h>
#include <Processors/QueryPlan/Streaming/DedupTransformStep.h>
#include <Processors/QueryPlan/Streaming/RowifyTransformStep.h>
#include <Processors/QueryPlan/Streaming/TimestampTransformStep.h>
#include <Processors/QueryPlan/Streaming/WindowAssignmentStep.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Random/StorageRandom.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageView.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
}
namespace Streaming
{
namespace
{
ContextMutablePtr createProxySubqueryContext(const ContextPtr & context, [[maybe_unused]] const SelectQueryInfo & query_info, bool is_streaming)
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
    TableFunctionDescriptionPtr table_func_desc_,
    TimestampFunctionDescriptionPtr timestamp_func_desc_,
    StoragePtr nested_proxy_storage_,
    String internal_name_,
    StoragePtr storage_,
    ASTPtr subquery_,
    DataStreamSemanticEx data_stream_semantic_,
    bool streaming_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , table_func_desc(std::move(table_func_desc_))
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , nested_proxy_storage(nested_proxy_storage_)
    , internal_name(std::move(internal_name_))
    , storage(storage_)
    , subquery(subquery_)
    , proxy_data_stream_semantic(data_stream_semantic_)
    , streaming(streaming_)
{
    if (context_->getSettingsRef().query_mode.value == "table")
        streaming = false;

    if (nested_proxy_storage)
        chassert(nested_proxy_storage->as<ProxyStream>());

    validateProxyChain();

    chassert(storage || subquery);
    chassert(!(storage && subquery));

    if (subquery)
    {
        /// Whether has GlobalAggregation in subquery
        /// FIXME, we interpreting subquery quite a few times already like in ProxyBase
        SelectQueryOptions options;
        options.setSubquery().analyze();

        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context_, options);

        has_global_aggr = interpreter_subquery->hasStreamingGlobalAggregation();
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
    if (!query_info.isStreaming())
    {
        if (windowType() == WindowType::Session || windowType() == WindowType::Hop)
            throw Exception(ErrorCodes::UNSUPPORTED, "{} window can only work with streaming query.", magic_enum::enum_name(windowType()));
    }

    if (internal_name == "changelog")
    {
        /// Push down tracking changes
        if (canTrackChangesFromInput(proxy_data_stream_semantic))
        {
            query_info.left_input_tracking_changes = true;
            query_info.changelog_query_drop_late_rows = std::any_cast<std::optional<bool>>(table_func_desc->func_ctx);
        }
        else
            /// Add additional ChangelogConvertTransform, so the input doesn't need tracking changes,
            query_info.left_input_tracking_changes = false;
    }

    auto required_column_names_for_proxy_storage = getRequiredColumnsForProxyStorage(column_names);

    doRead(
        query_plan,
        required_column_names_for_proxy_storage,
        storage_snapshot,
        query_info,
        context_,
        processed_stage,
        max_block_size,
        num_streams);

    /// Create step which reads from empty source if storage has no data.
    if (!query_plan.isInitialized())
    {
        /// Only in case when read from historical proxy storage
        chassert(!(query_info.isStreaming()));
        auto header = storage_snapshot->getSampleBlockForColumns(required_column_names_for_proxy_storage);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context_);
    }

    /// If it is distributed historical query, we will push down everything to remote servers, we don't need
    /// do streaming function here again
    bool distributed_historical_query
        = !streaming && (context_->getClientInfo().query_kind != ClientInfo::QueryKind::SECONDARY_QUERY && query_info.getCluster());
    if (!distributed_historical_query)
        buildStreamingFunctionQueryPlan(query_plan, column_names, query_info, storage_snapshot, context_);
}

void ProxyStream::doRead(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    ASTPtr current_subquery = subquery ? subquery->children[0]->clone() : nullptr;

    if (query_info.optimized_proxy_stream_query)
    {
        if (!query_info.optimized_proxy_stream_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized ProxyStream query");

        current_subquery = query_info.optimized_proxy_stream_query->clone();
    }

    if (current_subquery)
    {
        Streaming::rewriteSubquery(current_subquery->as<ASTSelectWithUnionQuery &>(), query_info);

        auto sub_context = createProxySubqueryContext(context_, query_info, isStreamingQuery());
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            current_subquery, sub_context, SelectQueryOptions().subquery().noModify(), column_names);

        interpreter_subquery->ignoreWithTotals();
        interpreter_subquery->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(sub_context);
        return;
    }

    if (auto * view = storage->as<StorageView>())
    {
        auto view_context = createProxySubqueryContext(context_, query_info, isStreamingQuery());
        view->read(query_plan, column_names, storage_snapshot, query_info, view_context, processed_stage, max_block_size, num_streams);
        query_plan.addInterpreterContext(view_context);
        return;
    }
    else if (auto * materialized_view = storage->as<StorageMaterializedView>())
        return materialized_view->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (auto * external_stream = storage->as<StorageExternalStream>())
        return external_stream->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (auto * random_stream = storage->as<StorageRandom>())
        return random_stream->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (auto * file_stream = storage->as<StorageFile>())
        return file_stream->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    else if (nested_proxy_storage)
        return nested_proxy_storage->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);

    auto * distributed = storage->as<StorageStream>();
    chassert(distributed);
    distributed->read(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

Names ProxyStream::getRequiredColumnsForProxyStorage(const Names & column_names) const
{
    if (nested_proxy_storage)
        return nested_proxy_storage->as<ProxyStream>()->getRequiredColumnsForProxyStorage(getRequiredInputs(column_names));

    return getRequiredInputs(column_names);
}

NamesAndTypesList ProxyStream::getVirtuals() const
{
    if (nested_proxy_storage)
        return nested_proxy_storage->getVirtuals();

    NamesAndTypesList virtuals;
    if (storage)
    {
        if (auto * distributed = storage->as<StorageStream>(); distributed)
        {
            if (streaming)
                virtuals = distributed->getVirtuals();
            else
                virtuals = distributed->getVirtualsHistorical();
        }
        else
            virtuals = storage->getVirtuals();
    }

    /// We may emit _tp_delta on the fly
    if (Streaming::isKeyValueStorage(dataStreamSemantic()) && !virtuals.contains(ProtonConsts::RESERVED_DELTA_FLAG))
        virtuals.emplace_back(ProtonConsts::RESERVED_DELTA_FLAG, DataTypeFactory::instance().get("int8"));

    return virtuals;
}

Names ProxyStream::getRequiredInputs(Names required_outputs) const
{
    /// Do nothing since the input shall track changes
    if (internal_name == "changelog" && canTrackChangesFromInput(proxy_data_stream_semantic))
        return required_outputs;

    auto required_inputs = std::move(required_outputs);
    if (table_func_desc)
    {
        /// Merge additional input columns
        std::ranges::for_each(table_func_desc->input_columns, [&](const auto & name) {
            if (std::ranges::find(required_inputs, name) == required_inputs.end())
                required_inputs.push_back(name);
        });
        /// Remove generated columns after current streaming table func
        std::erase_if(required_inputs, [this](const auto & name) { return table_func_desc->additional_result_columns.contains(name); });
    }

    if (timestamp_func_desc)
    {
        /// Merge additional input columns
        std::ranges::for_each(timestamp_func_desc->input_columns, [&](const auto & name) {
            if (std::ranges::find(required_inputs, name) == required_inputs.end())
                required_inputs.push_back(name);
        });
        /// Remove generated column `_tp_ts`
        std::erase_if(required_inputs, [](const auto & name) { return name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS; });
    }

    return required_inputs;
}

Block ProxyStream::checkAndGetOutputHeader(const Names & required_columns, const Block & input_header) const
{
    chassert(table_func_desc);
    Block output_header;
    output_header.reserve(required_columns.size());
    for (const auto & name : required_columns)
    {
        if (auto * input_col = input_header.findByName(name))
            output_header.insert(*input_col);
        else if (auto new_col = table_func_desc->additional_result_columns.tryGetByName(name); new_col.has_value())
            output_header.insert(ColumnWithTypeAndName{new_col->type->createColumn(), new_col->type, new_col->name});
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown input column '{}' of {} step", name, internal_name);
    }
    return output_header;
}

void ProxyStream::validateProxyChain() const
{
    /// We only support this sequence changelog(tumble(rowify(dedup(table(...), ...)), ...), ...)
    static const std::unordered_map<String, UInt8> func_name_order_map = {
        {"table", 1},
        {"python", 1},
        {"dedup", 2},
        {"rowify", 3},
        {"tumble", 4},
        {"hop", 4},
        {"session", 4},
        {"changelog", 5},
    };

    auto prev_name = internal_name;
    auto prev = func_name_order_map.at(prev_name);
    auto * next_proxy = nested_proxy_storage ? nested_proxy_storage->as<ProxyStream>() : nullptr;

    while (next_proxy)
    {
        auto cur = func_name_order_map.at(next_proxy->internal_name);
        if (cur >= prev)
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Wrap `{}` over `{}` is not supported. Use this wrap sequence: changelog -> tumble/hop/session -> rowify -> dedup -> table",
                prev_name,
                next_proxy->internal_name);

        if (next_proxy->nested_proxy_storage)
            next_proxy = next_proxy->nested_proxy_storage->as<ProxyStream>();
        else
            next_proxy = nullptr;
    }
}

void ProxyStream::buildStreamingFunctionQueryPlan(
    QueryPlan & query_plan,
    const Names & required_columns,
    const SelectQueryInfo & query_info,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context_) const
{
    if (nested_proxy_storage)
        nested_proxy_storage->as<ProxyStream>()->buildStreamingFunctionQueryPlan(
            query_plan, getRequiredInputs(required_columns), query_info, storage_snapshot, context_);

    /// changelog(dedup(table(stream), columns...), ...)
    if (internal_name == "changelog")
    {
        processChangelogStep(query_plan, required_columns, context_);
    }
    else if (internal_name == "dedup")
    {
        processDedupStep(query_plan, required_columns, context_);
    }
    else if (internal_name == "rowify")
    {
        processRowifyStep(query_plan, context_);
    }
    else
    {
        if (timestamp_func_desc)
            processTimestampStep(query_plan, query_info);

        /// tumble(dedup(table(stream), columns...), 5s)
        if (table_func_desc && table_func_desc->type != WindowType::None)
            processWindowAssignmentStep(query_plan, query_info, required_columns, storage_snapshot);
    }
}

void ProxyStream::processChangelogStep(QueryPlan & query_plan, const Names & required_columns, const ContextPtr & context_) const
{
    chassert(table_func_desc);

    /// Everything shall be in-place already since versioned_vk.read(...) will take care of adding
    /// ChangelogTransform step
    if (canTrackChangesFromInput(proxy_data_stream_semantic))
        return;

    auto output_header = checkAndGetOutputHeader(required_columns, query_plan.getCurrentDataStream().header);

    /// Get key columns
    String version_column;
    Strings key_column_names;
    auto drop_late_rows = std::any_cast<std::optional<bool>>(table_func_desc->func_ctx);
    if (drop_late_rows.has_value() && *drop_late_rows)
    {
        chassert(table_func_desc->argument_names.size() >= 2);

        version_column = table_func_desc->argument_names.back();

        /// NOTE: we cannot modify `table_func_desc`, because this operation may be performed twice
        /// When a select query with joined table needs to be analyzed again, the storage is cached and reused in `Context::executeTableFunction(...)`
        // /// Pop back the version column
        // table_func_desc->argument_names.pop_back();
        key_column_names.assign(table_func_desc->argument_names.begin(), --table_func_desc->argument_names.end());
    }
    else
        key_column_names.assign(table_func_desc->argument_names.begin(), table_func_desc->argument_names.end());

    /// Insert Changelog step
    const auto & settings_ref = context_->getSettingsRef();
    query_plan.addStep(std::make_unique<ChangelogConvertStep>(
        query_plan.getCurrentDataStream(),
        std::move(output_header),
        std::move(key_column_names),
        std::move(version_column),
        /*late_insert_overrides=*/true,
        settings_ref.default_hash_table.value,
        context_->getSpillDirForCurrentQuery("changelog"),
        settings_ref.max_hot_keys.value,
        settings_ref.kv_options.value,
        /*backfill_key_unique=*/false));
}

void ProxyStream::processDedupStep(QueryPlan & query_plan, const Names & required_columns, const ContextPtr & context_) const
{
    chassert(table_func_desc);
    auto output_header = checkAndGetOutputHeader(required_columns, query_plan.getCurrentDataStream().header);

    /// Insert dedup step
    query_plan.addStep(std::make_unique<DedupTransformStep>(
        query_plan.getCurrentDataStream(),
        output_header,
        table_func_desc,
        context_->getSettingsRef().default_hash_table.value,
        context_->getSpillDirForCurrentQuery("dedup"),
        context_->getSettingsRef().kv_options.value));
}

void ProxyStream::processRowifyStep(QueryPlan & query_plan, [[maybe_unused]] const ContextPtr & context_) const
{
    /// Insert rowify step
    query_plan.addStep(std::make_unique<RowifyTransformStep>(query_plan.getCurrentDataStream()));
}

void ProxyStream::processTimestampStep(QueryPlan & query_plan, const SelectQueryInfo & query_info) const
{
    chassert(timestamp_func_desc);

    auto output_header = query_plan.getCurrentDataStream().header;

    /// Add transformed timestamp column required by downstream pipe
    auto timestamp_col = timestamp_func_desc->expr->getSampleBlock().getByPosition(0);
    if (!output_header.has(timestamp_col.name))
    {
        timestamp_col.column = timestamp_col.type->createColumn();
        output_header.insert(std::move(timestamp_col));
    }

    chassert(query_info.seek_to_info);
    const auto & seek_to = query_info.seek_to_info->getSeekTo();
    bool backfill = !seek_to.empty() && seek_to != "latest" && seek_to != "earliest";

    query_plan.addStep(
        std::make_unique<TimestampTransformStep>(query_plan.getCurrentDataStream(), output_header, timestamp_func_desc, backfill));
}

void ProxyStream::processWindowAssignmentStep(
    QueryPlan & query_plan,
    const SelectQueryInfo & query_info,
    const Names & required_columns,
    const StorageSnapshotPtr & /*storage_snapshot*/) const
{
    chassert(table_func_desc && table_func_desc->type != WindowType::None);
    auto output_header = checkAndGetOutputHeader(required_columns, query_plan.getCurrentDataStream().header);

    query_plan.addStep(std::make_unique<WindowAssignmentStep>(
        query_plan.getCurrentDataStream(), std::move(output_header), query_info.streaming_window_params));
}

StorageSnapshotPtr ProxyStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    if (storage)
        return storage->getStorageSnapshot(metadata_snapshot, std::move(query_context));

    return IStorage::getStorageSnapshot(metadata_snapshot, std::move(query_context));
}

std::optional<String> ProxyStream::preferredColumn() const
{
    if (storage)
        return storage->preferredColumn();
    else if (nested_proxy_storage)
        return nested_proxy_storage->preferredColumn();

    return {};
}

std::optional<UInt64> ProxyStream::totalRows(const DB::Settings & settings) const
{
    if (storage)
        return storage->totalRows(settings);
    else if (nested_proxy_storage)
        return nested_proxy_storage->totalRows(settings);

    return {};
}

std::optional<UInt64> ProxyStream::approximateTotalRows(const DB::Settings & settings) const
{
    if (storage)
        return storage->approximateTotalRows(settings);
    else if (nested_proxy_storage)
        return nested_proxy_storage->approximateTotalRows(settings);

    return {};
}

WindowType ProxyStream::windowType() const
{
    WindowType type = table_func_desc != nullptr ? table_func_desc->type : WindowType::None;
    if (type == WindowType::None && nested_proxy_storage)
        type = nested_proxy_storage->as<ProxyStream>()->windowType();
    return type;
}

TableFunctionDescriptionPtr ProxyStream::getStreamingWindowFunctionDescription() const
{
    if (table_func_desc && table_func_desc->type != WindowType::None)
        return table_func_desc;

    if (nested_proxy_storage)
        return nested_proxy_storage->as<ProxyStream>()->getStreamingWindowFunctionDescription();

    return nullptr;
}

bool ProxyStream::isRemote() const
{
    if (storage)
        return storage->isRemote();

    return IStorage::isRemote();
}

bool ProxyStream::supportsParallelInsert() const
{
    if (storage)
        return storage->supportsParallelInsert();

    return IStorage::supportsParallelInsert();
}

bool ProxyStream::supportsIndexForIn() const
{
    if (storage)
        return storage->supportsIndexForIn();

    return IStorage::supportsIndexForIn();
}

bool ProxyStream::supportsSubcolumns() const
{
    if (storage)
        return storage->supportsSubcolumns();

    return IStorage::supportsSubcolumns();
}

bool ProxyStream::supportsStreamingQuery() const
{
    if (storage)
        return storage->supportsStreamingQuery();

    return IStorage::supportsStreamingQuery();
}

std::variant<StoragePtr, ASTPtr> ProxyStream::getProxyStorageOrSubquery() const
{
    if (storage)
        return storage;

    if (subquery)
        return subquery;

    UNREACHABLE();
}

bool ProxyStream::isProxyingSubqueryOrView() const
{
    if (subquery)
        return true;

    if (storage && storage->as<StorageView>())
        return true;

    return false;
}

DataStreamSemanticEx ProxyStream::dataStreamSemantic() const
{
    if (internal_name == "changelog")
        return DataStreamSemantic::Changelog;

    return proxy_data_stream_semantic; /// proxy data stream semantic
}

bool ProxyStream::hasLightweightDeletedMask() const
{
    if (storage)
        return storage->hasLightweightDeletedMask();

    return false;
}
}
}
