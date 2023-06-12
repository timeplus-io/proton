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
#include <Processors/QueryPlan/Streaming/TimestampTransformStep.h>
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
    TableFunctionDescriptionPtr table_func_desc_,
    TimestampFunctionDescriptionPtr timestamp_func_desc_,
    StoragePtr nested_proxy_storage_,
    String internal_name_,
    StoragePtr storage_,
    ASTPtr subquery_,
    bool streaming_)
    : IStorage(id_)
    , WithContext(context_->getGlobalContext())
    , table_func_desc(std::move(table_func_desc_))
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , nested_proxy_storage(nested_proxy_storage_)
    , internal_name(std::move(internal_name_))
    , storage(storage_)
    , subquery(subquery_)
    , streaming(streaming_)
    , log(&Poco::Logger::get(id_.getNameForLogs()))
{
    if (streaming)
        assert(table_func_desc);

    if (nested_proxy_storage)
        assert(nested_proxy_storage->as<ProxyStream>());

    validateProxyChain();

    if (windowType() == WindowType::SESSION && !isStreaming())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "session window can only work with streaming query.");

    assert(storage || subquery);
    assert(!(storage && subquery));

    if (subquery)
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
    auto reuired_column_names_for_proxy_storage = getRequiredColumnsForProxyStorage(column_names);
    doRead(query_plan, reuired_column_names_for_proxy_storage, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    buildStreamingFunctionQueryPlan(query_plan, column_names, query_info, storage_snapshot);
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
    /// If this storage is built from subquery
    assert(!(storage && subquery));
    if (query_info.proxy_stream_query)
    {
        if (!query_info.proxy_stream_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized ProxyStream query");

        auto sub_context = createProxySubqueryContext(context_, query_info, isStreaming());
        auto interpreter = std::make_unique<InterpreterSelectWithUnionQuery>(
            query_info.proxy_stream_query->clone(), sub_context, SelectQueryOptions().subquery().noModify(), column_names);

        interpreter->ignoreWithTotals();
        interpreter->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(sub_context);
        return;
    }

    if (subquery)
    {
        auto sub_context = createProxySubqueryContext(context_, query_info, isStreaming());
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], sub_context, SelectQueryOptions().subquery().noModify(), column_names);

        interpreter_subquery->ignoreWithTotals();
        interpreter_subquery->buildQueryPlan(query_plan);
        query_plan.addInterpreterContext(sub_context);
        return;
    }

    if (auto * view = storage->as<StorageView>())
    {
        auto view_context = createProxySubqueryContext(context_, query_info, isStreaming());
        view->read(
            query_plan, column_names, storage_snapshot, query_info, view_context, processed_stage, max_block_size, num_streams);
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
    else if (nested_proxy_storage)
        return nested_proxy_storage->read(
            query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);

    auto * distributed = storage->as<StorageStream>();
    assert(distributed);
    distributed->read(
        query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

Names ProxyStream::getRequiredColumnsForProxyStorage(const Names & column_names) const
{
    Names required_columns = mergeAdditionalRequiredColumnsAfterFunc(column_names);

    /// issue-1289
    /// If current table function is session / tumble / hop,
    /// we need drop STREAMING_WINDOW_START/END columns before forwarding the request
    /// since for these table functions, we will add window_start/end automatically
    /// Note even the inner is a table function or stream or view or MV which contains window_start/end,
    /// we choose the outer table function's window_start/end override the inner ones.
    if (windowType() != WindowType::NONE)
    {
        std::erase_if(required_columns, [](const auto & name) {
            return name == ProtonConsts::STREAMING_WINDOW_START || name == ProtonConsts::STREAMING_WINDOW_END
                || name == ProtonConsts::STREAMING_SESSION_START || name == ProtonConsts::STREAMING_SESSION_END
                || name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS;
        });
    }

    return required_columns;
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

Names ProxyStream::mergeAdditionalRequiredColumnsAfterFunc(Names required, const String & after_func_name) const
{
    if (after_func_name == internal_name)
        return required;

    if (nested_proxy_storage)
        required = nested_proxy_storage->as<ProxyStream>()->mergeAdditionalRequiredColumnsAfterFunc(std::move(required), after_func_name);

    if (timestamp_func_desc)
        std::ranges::for_each(timestamp_func_desc->input_columns, [&required](const auto & name) {
            if (std::ranges::find(required, name) == required.end())
                required.push_back(name);
        });

    if (table_func_desc)
        std::ranges::for_each(table_func_desc->input_columns, [&required](const auto & name) {
            if (std::ranges::find(required, name) == required.end())
                required.push_back(name);
        });

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

void ProxyStream::buildStreamingFunctionQueryPlan(
    QueryPlan & query_plan,
    const Names & required_columns_after_streaming_window,
    const SelectQueryInfo & query_info,
    const StorageSnapshotPtr & storage_snapshot) const
{
    if (nested_proxy_storage)
        nested_proxy_storage->as<ProxyStream>()->buildStreamingFunctionQueryPlan(
            query_plan, required_columns_after_streaming_window, query_info, storage_snapshot);

    /// dedup(table(stream), columns...)
    if (internal_name == "dedup")
        processDedupStep(query_plan, required_columns_after_streaming_window);
    else
    {
        if (timestamp_func_desc)
            processTimestampStep(query_plan, query_info);

        /// tumble(dedup(table(stream), columns...), 5s)
        if (table_func_desc && table_func_desc->type != WindowType::NONE)
            processWindowAssignmentStep(query_plan, query_info, required_columns_after_streaming_window, storage_snapshot);
    }
}

void ProxyStream::processDedupStep(QueryPlan & query_plan, const Names & required_columns_after_streaming_window) const
{
    assert(table_func_desc);

    auto required_columns_after_dedup = mergeAdditionalRequiredColumnsAfterFunc(required_columns_after_streaming_window, internal_name);

    const auto & input_header = query_plan.getCurrentDataStream().header;
    Block output_header;
    for (const auto & name : required_columns_after_dedup)
        output_header.insert(input_header.getByName(name));

    /// Insert dedup step
    query_plan.addStep(std::make_unique<DedupTransformStep>(query_plan.getCurrentDataStream(), output_header, table_func_desc));
}

void ProxyStream::processTimestampStep(QueryPlan & query_plan, const SelectQueryInfo & query_info) const
{
    assert(timestamp_func_desc);

    auto output_header = query_plan.getCurrentDataStream().header;

    /// Add transformed timestamp column required by downstream pipe
    auto timestamp_col = timestamp_func_desc->expr->getSampleBlock().getByPosition(0);
    assert(!output_header.findByName(timestamp_col.name));
    timestamp_col.column = timestamp_col.type->createColumn();
    output_header.insert(timestamp_col);

    assert(query_info.seek_to_info);
    const auto & seek_to = query_info.seek_to_info->getSeekTo();
    bool backfill = !seek_to.empty() && seek_to != "latest" && seek_to != "earliest";

    query_plan.addStep(
        std::make_unique<TimestampTransformStep>(query_plan.getCurrentDataStream(), output_header, timestamp_func_desc, backfill));
}

void ProxyStream::processWindowAssignmentStep(
    QueryPlan & query_plan,
    const SelectQueryInfo & query_info,
    const Names & required_columns_after_streaming_window,
    const StorageSnapshotPtr & storage_snapshot) const
{
    assert(table_func_desc && table_func_desc->type != WindowType::NONE);
    auto required_columns_after_window = mergeAdditionalRequiredColumnsAfterFunc(required_columns_after_streaming_window, internal_name);
    Block output_header = storage_snapshot->getSampleBlockForColumns(required_columns_after_window);

    query_plan.addStep(std::make_unique<WindowAssignmentStep>(
        query_plan.getCurrentDataStream(), std::move(output_header), query_info.streaming_window_params));
}

StorageSnapshotPtr ProxyStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    if (storage)
        return storage->getStorageSnapshot(metadata_snapshot, std::move(query_context));

    return IStorage::getStorageSnapshot(metadata_snapshot, std::move(query_context));
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
