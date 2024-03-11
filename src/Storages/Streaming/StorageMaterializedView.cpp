#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageStream.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/DiskUtilChecker.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Storages/AlterCommands.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageFactory.h>
#include <Common/ErrorCodes.h>
#include <Common/ProfileEvents.h>
#include <Common/ProtonCommon.h>
#include <Common/checkStackSize.h>
#include <Common/setThreadName.h>

#include <ranges>


namespace ProfileEvents
{
extern const Event OSCPUWaitMicroseconds;
extern const Event OSCPUVirtualTimeMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_QUERY;
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
extern const int RESOURCE_NOT_INITED;
extern const int QUERY_WAS_CANCELLED;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int DIRECTORY_DOESNT_EXIST;
extern const int UNEXPECTED_ERROR_CODE;
}


namespace
{
String generateInnerTableName(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner.target-id." + toString(view_id.uuid);
    return ".inner.target." + view_id.getTableName();
}

String generateInnerQueryID(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner.query-id.from-" + toString(view_id.uuid);
    return ".inner.query-id.from-" + view_id.getTableName();
}
}

class CheckMaterializedViewValidTransform final : public ISimpleTransform
{
public:
    CheckMaterializedViewValidTransform(const Block & header_, const StorageMaterializedView & view_)
        : ISimpleTransform(header_, header_, false, ProcessorID::CheckMaterializedViewValidTransformID), view(view_)
    {
    }

    String getName() const override { return "CheckMaterializedViewValidTransform"; }

protected:
    void transform(Chunk &) override { view.checkValid(); }

private:
    const StorageMaterializedView & view;
};


StorageMaterializedView::State::~State()
{
    terminate();
}

void StorageMaterializedView::State::terminate()
{
    /// Cancel pipeline executing first
    is_cancelled = true;

    if (thread.joinable())
        thread.join();

    updateStatus(State::UNKNOWN);

    err.store(ErrorCodes::OK);
}

void StorageMaterializedView::State::updateStatus(StorageMaterializedView::State::ThreadStatus status)
{
    thread_status.store(status, std::memory_order_relaxed);
    thread_status.notify_all();
}

void StorageMaterializedView::State::waitStatusUntil(StorageMaterializedView::State::ThreadStatus target_status) const
{
    auto current_status = thread_status.load(std::memory_order_relaxed);
    while (current_status < target_status)
    {
        thread_status.wait(current_status, std::memory_order_relaxed);
        current_status = thread_status.load(std::memory_order_relaxed);
    }

    checkException();
}

void StorageMaterializedView::State::setException(int code, const String & msg, bool log_error)
{
    err_msg = msg;
    err = code;

    if (log_error)
        LOG_ERROR(
            log,
            "{}: {} (Background status: {})",
            ErrorCodes::getName(code),
            msg,
            magic_enum::enum_name(thread_status.load(std::memory_order_relaxed)));
}

void StorageMaterializedView::State::checkException(const String & msg_prefix) const
{
    auto err_code = err.load();
    if (err_code != ErrorCodes::OK)
        throw Exception(
            err_code,
            "{} {} (Background status: {})",
            msg_prefix,
            err_msg,
            magic_enum::enum_name(thread_status.load(std::memory_order_relaxed)));
}

StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_,
    bool is_virtual_)
    : IStorage(table_id_)
    , WithMutableContext(local_context->getGlobalContext())
    , log(&Poco::Logger::get(fmt::format("StorageMaterializedView ({})", table_id_.getFullTableName())))
    , is_attach(attach_)
    , is_virtual(is_virtual_)
    , background_state{.log = log}
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    /// If `INTO [target stream]` is not specified, use inner stream
    has_inner_table = query.to_table_id.empty();

    auto select = SelectQueryDescription::getSelectQueryFromASTForView(query.select->clone(), local_context);

    storage_metadata.setSelectQuery(select);
    setInMemoryMetadata(storage_metadata);

    if (!attach_)
        validateInnerQuery(storage_metadata, local_context);

    bool point_to_itself_by_uuid = has_inner_table && query.to_inner_uuid != UUIDHelpers::Nil && query.to_inner_uuid == table_id_.uuid;
    bool point_to_itself_by_name = !has_inner_table && query.to_table_id.database_name == table_id_.database_name
        && query.to_table_id.table_name == table_id_.table_name;
    if (point_to_itself_by_uuid || point_to_itself_by_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view {} cannot point to itself", table_id_.getFullTableName());

    if (!has_inner_table)
    {
        target_table_id = query.to_table_id;

        if (!attach_)
        {
            auto target_table = tryGetTargetTable();
            if (!target_table)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Target stream is not found", target_table_id.getFullTableName());

            if (!target_table->as<StorageStream>() && !target_table->as<StorageExternalStream>()
                && !target_table->as<StorageExternalTable>())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "MaterializedView doesn't support target storage is {}", target_table->getName());
        }
    }
    else if (attach_)
    {
        /// If this is an ATTACH request, then the internal stream must already be created
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
    }
    else
    {
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);

        /// In virtual scenarios, inner stream is supposed to already exist
        if (is_virtual)
            return;

        /// We will create a query to create an internal stream
        createInnerTable();
    }
}

void StorageMaterializedView::validateInnerQuery(const StorageInMemoryMetadata & storage_metadata, const ContextPtr & local_context) const
{
    /// Validate if the inner query is a streaming query. Only streaming query is supported for now
    auto select_context = Context::createCopy(local_context);
    select_context->makeQueryContext();
    select_context->setCurrentQueryId(""); /// generate random query_id

    auto description = storage_metadata.getSelectQuery();
    InterpreterSelectWithUnionQuery select_interpreter(description.inner_query, select_context, SelectQueryOptions().analyze());
    if (!select_interpreter.isStreamingQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Materialized view doesn't support historical select query");

    if (std::ranges::find(description.select_table_ids, getStorageID()) != description.select_table_ids.end())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} {} cannot select from itself", getName(), getStorageID().getFullTableName());
}

StorageMaterializedView::~StorageMaterializedView()
{
    shutdown();
}

///                             /- (inner_target_table)
/// InMemoryTable + TargetTable
///                             \- (into_target_table)
void StorageMaterializedView::startup()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_id = getStorageID();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().addDependency(select_table_id, storage_id);

    DatabaseCatalog::instance().addDependency(target_table_id, storage_id);

    /// Sync target table settings and ttls
    updateStorageSettingsAndTTLs();

    if (is_virtual)
        return;

    initBackgroundState();

    /// NOTE: If it's an Create request, we should wait for done of startup background pipeline,
    /// so that, we can report the error if has exception (e.g. bad query)
    if (!is_attach)
    {
        auto start = MonotonicMilliseconds::now();
        background_state.waitStatusUntil(State::EXECUTING_PIPELINE);
        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "Took {} ms to wait for built background pipeline during matierialized view '{}' startup",
            end - start,
            getStorageID().getFullTableName());
    }
}

void StorageMaterializedView::shutdown()
{
    if (shutdown_called.test_and_set())
        return;

    background_state.terminate();

    auto storage_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().removeDependency(select_table_id, storage_id);

    DatabaseCatalog::instance().removeDependency(target_table_id, storage_id);
}

void StorageMaterializedView::checkDependencies() const
{
    std::vector<StoragePtr> waiting_storages;

    /// Check target stream
    if (auto target = getTargetTable(); !target->isReady())
        waiting_storages.emplace_back(std::move(target));

    /// Check source storages
    auto context = getContext();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
    {
        if (unlikely(select_table_id == getStorageID()))
            continue;

        auto source_storage = DatabaseCatalog::instance().getTable(select_table_id, context);
        if (!source_storage->isReady())
            waiting_storages.emplace_back(std::move(source_storage));
    }

    if (waiting_storages.empty())
        return;

    auto waiting_storages_names_v
        = waiting_storages | std::views::transform([](const auto & storage) { return storage->getStorageID().getFullTableName(); });

    throw Exception(
        ErrorCodes::RESOURCE_NOT_INITED,
        "The dependencies '{}' of matierialized view '{}' are not ready yet",
        fmt::join(waiting_storages_names_v, ", "),
        getStorageID().getFullTableName());
}

void StorageMaterializedView::initBackgroundState()
{
    background_state.thread = ThreadFromGlobalPool{[this]() {
        try
        {
            setThreadName("MVBgQueryEx");

            auto local_context = Context::createCopy(getContext());
            CurrentThread::QueryScope query_scope(local_context);
            ExecuteMode exec_mode = is_attach ? ExecuteMode::RECOVER : ExecuteMode::SUBSCRIBE;

            /// FIXME: limit retry times ?
            size_t retry_times = 0;
            /// Keep the last SNs of the stream source during the last retries.
            std::deque<std::vector<Int64>> last_sns_of_streaming_sources;
            std::vector<Int64> recovered_sns_of_streaming_sources;
            while (1)
            {
                if (background_state.is_cancelled)
                    break;

                ++retry_times;

                BlockIO io;
                try
                {
                    auto current_status = background_state.thread_status.load(std::memory_order_relaxed);
                    switch (current_status)
                    {
                        case State::UNKNOWN:
                            [[fallthrough]];
                        case State::CHECKING_DEPENDENCIES: {
                            background_state.updateStatus(State::CHECKING_DEPENDENCIES);
                            /// During server bootstrap, we shall startup all tables in parallel, so here
                            /// needs validity check underlying tables.
                            checkDependencies();
                            [[fallthrough]];
                        }
                        case State::BUILDING_PIPELINE: {
                            background_state.updateStatus(State::BUILDING_PIPELINE);
                            local_context->setSetting(
                                "exec_mode", exec_mode == ExecuteMode::RECOVER ? String("recover") : String("subscribe"));
                            io = buildBackgroundPipeline(local_context);
                            assert(io.pipeline.initialized());
                            [[fallthrough]];
                        }
                        case State::EXECUTING_PIPELINE: {
                            background_state.updateStatus(State::EXECUTING_PIPELINE);
                            background_state.err = ErrorCodes::OK; /// Reset the status is ok after recovered
                            io.pipeline.setExecuteMode(exec_mode);
                            if (!recovered_sns_of_streaming_sources.empty())
                                io.pipeline.resetSNsOfStreamingSources(recovered_sns_of_streaming_sources);
                            executeBackgroundPipeline(io, local_context);
                            break;
                        }
                        default:
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR, "No support background state: {}", magic_enum::enum_name(current_status));
                    }

                    break; /// Cancelled normally, no retry
                }
                catch (Exception & e)
                {
                    auto current_status = background_state.thread_status.load(std::memory_order_relaxed);
                    /// For create request, no retry if built pipeline fails
                    /// and update status to `FATAL`, it will wake up the blocked thread (startup()) and then check whether exception exists later
                    if (unlikely(retry_times == 1 && !is_attach && current_status < State::EXECUTING_PIPELINE))
                    {
                        background_state.setException(e.code(), e.message());
                        background_state.updateStatus(State::FATAL);
                        return;
                    }

                    /// FIXME: Add more (un)retriable error handling
                    auto err_code = e.code();
                    if (err_code == ErrorCodes::RESOURCE_NOT_INITED)
                    {
                        /// Retry to wait for dependencies
                        /// The first check of dependencies will most likely fail, so we skip logging error once.
                        bool log_error = (retry_times > 1);
                        background_state.setException(err_code, fmt::format("{}, {}th wait for ready", e.what(), retry_times), log_error);
                        std::this_thread::sleep_for(recheck_dependencies_interval);
                    }
                    else if (err_code == ErrorCodes::DIRECTORY_DOESNT_EXIST)
                    {
                        /// Compatibility case for attach old materialized views:
                        /// Failed to recover with err msg "Failed to recover checkpoint since checkpoint directory ... doesn't exist"
                        /// FIXME: match msg ?
                        exec_mode = ExecuteMode::SUBSCRIBE;
                        background_state.setException(
                            err_code,
                            fmt::format(
                                "Retry {}th re-subscribe to background pipeline of matierialized view '{}'. Background runtime error: {}",
                                retry_times,
                                getStorageID().getFullTableName(),
                                e.what()));
                    }
                    else
                    {
                        background_state.setException(
                            err_code,
                            fmt::format(
                                "Wait for {}th recovering background pipeline of matierialized view '{}'. Background runtime error: {}",
                                retry_times,
                                getStorageID().getFullTableName(),
                                e.what()));

                        /// Allow skipping some SNs to avoid permanent errors
                        auto current_sns_of_streaming_source = io.pipeline.getLastSNsOfStreamingSources();
                        if (last_sns_of_streaming_sources.size() >= 2)
                        {
                            std::vector<Int64> next_sns;
                            next_sns.reserve(current_sns_of_streaming_source.size());
                            bool need_reset_recovered_sns = false;
                            for (size_t i = 0; i < current_sns_of_streaming_source.size(); ++i)
                            {
                                /// If the current SN fails three times in a row, recovery will begin from the next SN.
                                if (std::ranges::all_of(last_sns_of_streaming_sources, [&](const auto & sns) {
                                        return sns[i] == current_sns_of_streaming_source[i];
                                    }))
                                {
                                    need_reset_recovered_sns = true;
                                    next_sns.emplace_back(current_sns_of_streaming_source[i] + 1);
                                }
                                else
                                    next_sns.emplace_back(-1);
                            }

                            if (!need_reset_recovered_sns)
                                next_sns.clear();
                            else
                                LOG_INFO(
                                    background_state.log,
                                    "Skip some SNs to avoid permanent errors, next SNs: {}",
                                    fmt::join(next_sns | std::views::transform([](auto sn) { return std::to_string(sn); }), ", "));

                            recovered_sns_of_streaming_sources.swap(next_sns);

                            last_sns_of_streaming_sources.pop_front();
                        }

                        last_sns_of_streaming_sources.emplace_back(std::move(current_sns_of_streaming_source));

                        /// For some queries that always go wrong, we don’t want to recover too frequently. Now wait 5s, 10s, 15s, ... 30s, 30s for each time
                        std::this_thread::sleep_for(recover_interval * std::min<size_t>(retry_times, 6));

                        /// Retry recovering with recover mode
                        if (current_status == State::EXECUTING_PIPELINE)
                            exec_mode = ExecuteMode::RECOVER;
                    }

                    /// NOTE: If failed to executing pipeline, we need to re-build an new pipeline,
                    /// since the processors of current failed pipeline are invalid.
                    if (current_status == State::EXECUTING_PIPELINE)
                        background_state.updateStatus(State::BUILDING_PIPELINE);
                }
            }
        }
        catch (...)
        {
            auto e = std::current_exception();
            background_state.setException(getExceptionErrorCode(e), getExceptionMessage(e, false));
        }
    }};
}

BlockIO StorageMaterializedView::buildBackgroundPipeline(ContextMutablePtr local_context)
{
    BlockIO io;

    auto target_table = getTargetTable();
    auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    local_context->setCurrentQueryId(generateInnerQueryID(getStorageID())); /// Use a query_id bound to the uuid of mv
    local_context->setInternalQuery(false); /// We like to log materialized query like a regular one
    local_context->setInsertionTable(target_table->getStorageID());
    local_context->setIngestMode(IngestMode::SYNC); /// No need async mode status for internal ingest

    auto & inner_query = metadata_snapshot->getSelectQuery().inner_query;
    io.process_list_entry = local_context->getProcessList().insert(serializeAST(*inner_query), inner_query.get(), local_context);
    local_context->setProcessListElement(&io.process_list_entry->get());
    CurrentThread::get().performance_counters[ProfileEvents::OSCPUWaitMicroseconds] = 0;
    CurrentThread::get().performance_counters[ProfileEvents::OSCPUVirtualTimeMicroseconds] = 0;

    /// NOTE: Here we build two stages `select` + `insert` instead of directly building `insert select`,
    /// so that the pipeline can be adjusted more flexibly
    InterpreterSelectWithUnionQuery select_interpreter(inner_query, local_context, SelectQueryOptions());

    /// [Pipeline]: `Source` -> `Converting` -> `Materializing const` -> `target_table`
    auto pipeline_builder = select_interpreter.buildQueryPipeline();
    pipeline_builder.resize(1);
    const auto & source_header = pipeline_builder.getHeader();

    Block target_header;
    Names insert_columns;
    ActionsDAG::MatchColumnsMode match_mode = ActionsDAG::MatchColumnsMode::Position;

    if (!has_inner_table)
    {
        /// Has specified `INTO [target stream]`
        /// The `select` output header may not match the target stream's schema

        target_header.reserve(source_header.columns());
        /// Insert columns only returned by select query
        const auto & target_table_columns = target_metadata_snapshot->getColumns();
        auto target_storage_header{target_metadata_snapshot->getSampleBlock()};
        for (const auto & source_column : source_header)
        {
            /// Skip columns which target storage doesn't have
            if (target_table_columns.hasPhysical(source_column.name))
            {
                insert_columns.emplace_back(source_column.name);
                target_header.insert(target_storage_header.getByName(source_column.name));
            }
        }
        /// We will need use by name since we may have less columns after filtering
        match_mode = ActionsDAG::MatchColumnsMode::Name;
    }
    else
    {
        /// Internally inner table. The inner storage header shall be identical as the source header
        insert_columns = source_header.getNames();
        target_header = metadata_snapshot->getSampleBlock();

        /// When inserted columns don't contains `_tp_time`, we will skip it, which will be filled by default
        if (!source_header.has(ProtonConsts::RESERVED_EVENT_TIME) && target_header.has(ProtonConsts::RESERVED_EVENT_TIME))
            target_header.erase(ProtonConsts::RESERVED_EVENT_TIME);
    }

    if (!blocksHaveEqualStructure(source_header, target_header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            source_header.getColumnsWithTypeAndName(), target_header.getColumnsWithTypeAndName(), match_mode);

        pipeline_builder.addTransform(std::make_shared<ExpressionTransform>(
            source_header,
            std::make_shared<ExpressionActions>(
                std::move(converting), ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes))));
    }

    /// Materializing const columns
    pipeline_builder.addTransform(std::make_shared<MaterializingTransform>(pipeline_builder.getHeader()));

    /// Sink to target table
    InterpreterInsertQuery interpreter(nullptr, local_context, false, false, false);
    /// FIXME, thread status
    auto out_chain
        = interpreter.buildChain(target_table, target_metadata_snapshot, insert_columns, nullptr, nullptr, pipeline_builder.isStreaming());
    out_chain.addStorageHolder(target_table);
    auto resources = out_chain.detachResources();

    pipeline_builder.addChain(std::move(out_chain));

    pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<EmptySink>(cur_header);
    });

    io.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    io.pipeline.addResources(std::move(resources));
    io.pipeline.setProgressCallback(local_context->getProgressCallback());
    io.pipeline.setProcessListElement(local_context->getProcessListElement());

    /// NOTE: We must use enough threads to process streaming query, otherwise, there is no idle thread to process Sink, all threads are used for select processing (Especially join queries)
    // io.pipeline.setNumThreads(std::min<size_t>(io.pipeline.getNumThreads(), local_context->getSettingsRef().max_threads));
    return io;
}

void StorageMaterializedView::executeBackgroundPipeline(BlockIO & io, ContextMutablePtr local_context)
{
    assert(io.process_list_entry);
    if ((*io.process_list_entry)->isKilled())
        throw Exception(
            ErrorCodes::QUERY_WAS_CANCELLED,
            "The background pipeline for materialized view {} is killed before execution, it's abnormal.",
            getStorageID().getFullTableName());

    LOG_INFO(log, "Executing background pipeline for materialized view {}", getStorageID().getFullTableName());
    CompletedPipelineExecutor executor(io.pipeline);
    executor.setCancelCallback(
        [this] { return background_state.is_cancelled.load(); }, local_context->getSettingsRef().interactive_delay / 1000);
    executor.execute();

    /// If the pipeline is finished, we need check whether it's cancelled or not. If not, it's abnormal.
    /// For exmaple:
    /// A background query likes stream join table, and the table has no data, the background pipeline will be finished immediately.
    /// In this case, we need throw a exception to notify users and retry it.
    if (!((*io.process_list_entry)->isKilled() || background_state.is_cancelled.load()))
        throw Exception(
            ErrorCodes::UNEXPECTED_ERROR_CODE,
            "The background pipeline for materialized view {} finished unexpectedly, it's abnormal.",
            getStorageID().getFullTableName());
}

void StorageMaterializedView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const size_t num_streams)
{
    /// The behavior of querying a MV is same as querying the underlying stream`
    /// In some cases, the view background thread has exception, we check it before users access this view
    checkValid();

    auto storage = getTargetTable();

    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME});

    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto target_storage_snapshot = storage->getStorageSnapshot(target_metadata_snapshot, local_context);

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(
        query_plan, column_names, target_storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        if (query_info.syntax_analyzer_result->streaming)
        {
            // This check is so weird, disable it for now
            /// Add valid check of the view
            /// If not check, when the view go bad, the streaming query of the target stream will be blocked indefinitely
            /// since there is no ingestion on background.
            /// pipe.addTransform(std::make_shared<CheckMaterializedViewValidTransform>(header, *this));
            /// query_plan.addStep(std::move(read_step));
        }

        query_plan.addStorageHolder(storage);
        query_plan.addTableLock(std::move(lock));
    }
}

void StorageMaterializedView::preDrop()
{
    shutdown();

    /// Remove checkpoint of the inner query
    CheckpointCoordinator::instance(getContext()).removeCheckpoint(generateInnerQueryID(getStorageID()));
}

void StorageMaterializedView::drop()
{
    dropInnerTableIfAny(true, getContext());
}

void StorageMaterializedView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (has_inner_table && target_table_id)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);
}

void StorageMaterializedView::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    getTargetTable()->alter(commands, context_, alter_lock_holder);
    updateStorageSettingsAndTTLs();
}

void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr ctx) const
{
    if (!has_inner_table)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Materialized view with specified target stream can't be altered.");

    if (!std::ranges::all_of(commands, [&](const AlterCommand & c) {
            return c.isSettingsAlter() || c.type == AlterCommand::MODIFY_TTL || c.type == AlterCommand::REMOVE_TTL;
        }))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only materialized view's settings/table_ttl can be altered.");

    getTargetTable()->checkAlterIsPossible(commands, ctx);
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    IStorage::renameInMemory(new_table_id);

    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().updateDependency(select_table_id, old_table_id, select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();

    return DatabaseCatalog::instance().getTable(target_table_id, getContext());
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();

    return DatabaseCatalog::instance().tryGetTable(target_table_id, getContext());
}

void StorageMaterializedView::checkValid() const
{
    /// check disk quota
    DiskUtilChecker::instance(getContext()).check();

    background_state.checkException("Bad MaterializedView, please wait for auto-recovery or restart/recreate manually:");

    if (!isReady())
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Background thread of '{}' are initializing", getStorageID().getFullTableName());
}

bool StorageMaterializedView::isReady() const
{
    return is_virtual || background_state.thread_status.load(std::memory_order_relaxed) > State::CHECKING_DEPENDENCIES;
}

NamesAndTypesList StorageMaterializedView::getVirtuals() const
{
    return getTargetTable()->getVirtuals();
}

void StorageMaterializedView::createInnerTable()
{
    assert(!is_attach && !is_virtual && has_inner_table);

    /// Init inner memory table and inner target table
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto local_context = Context::createCopy(getContext());
    local_context->makeQueryContext();
    local_context->setCurrentQueryId(""); // generate random query_id

    int max_retries = 10;
    while (max_retries-- > 0)
    {
        try
        {
            doCreateInnerTable(metadata_snapshot, local_context);
            return;
        }
        catch (...)
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            LOG_DEBUG(log, "'{}' waiting for inner stream ready", getStorageID().getFullTableName());
        }
    }

    if (max_retries <= 0)
    {
        LOG_ERROR(log, "Failed to create inner stream for Materialized view '{}'", getStorageID().getFullTableName());
        throw Exception(
            ErrorCodes::RESOURCE_NOT_INITED, "Failed to create inner stream for Materialized view '{}'", getStorageID().getFullTableName());
    }
}

void StorageMaterializedView::doCreateInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr local_context)
{
    assert(has_inner_table && target_table_id);

    /// If there is a Create request, then we need create the target inner table.
    /// Create inner target table query
    ///   create stream <inner_target_table_id> (view_properties) engine = Stream(1, 1, rand());
    /// FIXME: In future, add order clause or remove engine ?
    auto manual_create_query = std::make_shared<ASTCreateQuery>();
    manual_create_query->setDatabase(target_table_id.getDatabaseName());
    manual_create_query->setTable(target_table_id.getTableName());
    manual_create_query->uuid = target_table_id.uuid;

    auto columns_ast = InterpreterCreateQuery::formatColumns(metadata_snapshot->getColumns());
    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, columns_ast);

    auto new_storage = std::make_shared<ASTStorage>();
    auto engine = makeASTFunction(
        "Stream", std::make_shared<ASTLiteral>(UInt64(1)), std::make_shared<ASTLiteral>(UInt64(1)), makeASTFunction("rand"));
    engine->no_empty_args = true;
    new_storage->set(new_storage->engine, engine);

    manual_create_query->set(manual_create_query->columns_list, new_columns_list);
    manual_create_query->set(manual_create_query->storage, new_storage);

    InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    target_table_id = DatabaseCatalog::instance()
                          .getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, getContext())
                          ->getStorageID();
}

void StorageMaterializedView::updateStorageSettingsAndTTLs()
{
    auto meta = getInMemoryMetadata();
    auto target_metadata = getTargetTable()->getInMemoryMetadataPtr();
    meta.setSettingsChanges(target_metadata->getSettingsChanges());
    meta.setTableTTLs(target_metadata->getTableTTLs());
    // meta.setColumnTTLs(target_metadata->getColumnTTLs()); /// Not support
    setInMemoryMetadata(meta);
}

StorageSnapshotPtr StorageMaterializedView::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshot(metadata_snapshot, query_context);
}

MergeTreeSettingsPtr StorageMaterializedView::getSettings() const
{
    if (auto * stream = getTargetTable()->as<StorageStream>())
        return stream->getSettings();

    return nullptr;
}

Streaming::DataStreamSemanticEx StorageMaterializedView::dataStreamSemantic() const
{
    return getTargetTable()->dataStreamSemantic();
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args) {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_id, args.getLocalContext(), args.query, args.columns, args.attach, args.is_virtual);
    });
}
}
