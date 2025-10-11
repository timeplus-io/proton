#include <Storages/MatView/StorageMaterializedView.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/Common/Constants.h>
#include <Core/RecoveryPolicy.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/MaterializedViewDeadLetterQueue.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/formatAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/ClockUtils.h>
#include <base/getThreadId.h>
#include <Common/ProtonCommon.h>
#include <Common/Stopwatch.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

#include <absl/container/flat_hash_set.h>
#include <boost/smart_ptr/make_shared.hpp>

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
extern const int INCORRECT_QUERY;
extern const int RESOURCE_NOT_INITED;
extern const int QUERY_WAS_CANCELLED;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int CHECKPOINT_DOESNT_EXIST;
extern const int RECOVER_CHECKPOINT_FAILED;
extern const int TIMEOUT_EXCEEDED;
extern const int PIPELINE_FINISHED_EARLY;
extern const int INGEST_TIMEOUT;
extern const int SOCKET_TIMEOUT;
extern const int NETWORK_ERROR;
extern const int CANNOT_CONNECT_SERVER;
extern const int INTERNAL_ERROR;
extern const int MATERIALIZED_VIEW_STATUS_CHANGED;
extern const int MATERIALIZED_VIEW_STOPPED;
extern const int SEQUENCE_COMPACTED_AWAY;

extern const int CANNOT_PARSE_TEXT;
extern const int CANNOT_PARSE_DATE;
extern const int CANNOT_PARSE_DATETIME;
extern const int CANNOT_PARSE_NUMBER;
extern const int CANNOT_PARSE_UUID;
extern const int CANNOT_PARSE_BOOL;
extern const int CANNOT_PARSE_IPV4;
extern const int CANNOT_PARSE_IPV6;
extern const int CANNOT_PARSE_YAML;
extern const int CANNOT_PARSE_ELF;
extern const int CANNOT_PARSE_DWARF;
extern const int CANNOT_PARSE_CAPN_PROTO_SCHEMA;
extern const int CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT;
extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
extern const int CANNOT_PARSE_QUOTED_STRING;
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
extern const int CANNOT_PARSE_VALUE;
extern const int CANNOT_READ_ARRAY_FROM_TEXT;
extern const int CANNOT_CONVERT_CHARSET;
}

namespace
{
absl::flat_hash_set<int> KNOWN_POISON_EVENTS_ERRORS = {
    ErrorCodes::CANNOT_PARSE_TEXT,
    ErrorCodes::CANNOT_PARSE_DATE,
    ErrorCodes::CANNOT_PARSE_DATETIME,
    ErrorCodes::CANNOT_PARSE_NUMBER,
    ErrorCodes::CANNOT_PARSE_UUID,
    ErrorCodes::CANNOT_PARSE_BOOL,
    ErrorCodes::CANNOT_PARSE_IPV4,
    ErrorCodes::CANNOT_PARSE_IPV6,
    ErrorCodes::CANNOT_PARSE_YAML,
    ErrorCodes::CANNOT_PARSE_ELF,
    ErrorCodes::CANNOT_PARSE_DWARF,
    ErrorCodes::CANNOT_PARSE_CAPN_PROTO_SCHEMA,
    ErrorCodes::CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT,
    ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE,
    ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
    ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
    ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA,
    ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING,
    ErrorCodes::CANNOT_PARSE_VALUE,
    ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT,
    ErrorCodes::CANNOT_CONVERT_CHARSET,
};

bool isPermanentError(int err)
{
    return err == ErrorCodes::RECOVER_CHECKPOINT_FAILED || err == ErrorCodes::SEQUENCE_COMPACTED_AWAY;
}

void resetSNForStreamingSources(const Streaming::StreamingSourcePtrs & streaming_sources, const std::vector<Int64> & sns)
{
    if (sns.size() != streaming_sources.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of sequence numbers doesn't match number of streaming sources");

    for (auto && [streaming_source, sn] : std::views::zip(streaming_sources, sns))
        if (sn >= 0)
            streaming_source->resetStartSN(sn);
}

struct StreamingSourcesDescription
{
    Strings names;
    Strings descriptions;
    std::vector<Streaming::SequenceRange> processed_sns_range;
    std::vector<Int64> checkpointed_sns;

    String dumpWithSNs(const std::vector<Int64> & sns) const
    {
        if (names.empty())
            return "empty source";

        /// A example: "StreamingSource(sn=1,uuid=xxx,shard=0);KafkaSource(sn=2,topic=xxx,partition=0); processed_sns=1,2; checkpointed_sns=1,1"
        WriteBufferFromOwnString wb;
        if (sns.empty())
        {
            for (auto [name, description] : std::views::zip(names, descriptions))
                wb << name << "(" << description << ");";
        }
        else
        {
            if (sns.size() != names.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of sequence numbers doesn't match number of streaming sources");

            /// Use last checkpointed sn instead of invalid sn
            for (auto [name, description, sn, checkpointed_sn] : std::views::zip(names, descriptions, sns, checkpointed_sns))
                wb << name << "(sn=" << (sn >= 0 ? sn : checkpointed_sn) << "," << description << ");";
        }

        wb << " processed_sns="
           << fmt::format(
                  "{}",
                  fmt::join(
                      processed_sns_range
                          | std::views::transform([](const auto & sn_range) { return fmt::format("{}-{}", sn_range.start, sn_range.end); }),
                      ","));

        wb << "; checkpointed_sns=" << fmt::format("{}", fmt::join(checkpointed_sns, ","));

        return wb.str();
    }
};

StreamingSourcesDescription getStreamingSourcesDescription(const Streaming::StreamingSourcePtrs & streaming_sources)
{
    StreamingSourcesDescription res;
    res.names.reserve(streaming_sources.size());
    res.descriptions.reserve(streaming_sources.size());
    res.processed_sns_range.reserve(streaming_sources.size());
    res.checkpointed_sns.reserve(streaming_sources.size());
    for (const auto & streaming_source : streaming_sources)
    {
        res.names.emplace_back(streaming_source->getName());
        res.descriptions.emplace_back(streaming_source->getDescription());
        res.processed_sns_range.emplace_back(streaming_source->lastProcessedSNRange());
        res.checkpointed_sns.emplace_back(streaming_source->lastCheckpointSN());
    }
    return res;
}
}

void StorageMaterializedView::PipelineState::shutdown() noexcept
{
    cancel(/*abort=*/!mv.stopped.test());

    if (executor.joinable())
    {
        /// This could happen due to certain bugs or conditions where the pipeline may not finish or take too long.
        /// If the executor is still running after the maximum wait time (2s), it is detached to allow it to continue in the background.
        if (!waitPipelineFinished(/*max_wait_ms=*/2000))
        {
            LOG_ERROR(
                logger,
                "Pipeline didn't finish within the expected time (2s) after cancelled. It will be detached to continue in the background "
                "(tid={})",
                executor_tid.load());
            executor.detach();
        }
        else
        {
            executor.join();
        }
    }
    LOG_INFO(logger, "Background pipeline shutdown");
}

void StorageMaterializedView::PipelineState::cancel(bool abort) noexcept
{
    if (is_cancelled.test_and_set())
        return;

    /// Wake up the thread waiting
    {
        validated.store(true);
        validated.notify_all();

        updateStatus(abort ? Status::Error : Status::None);

        std::lock_guard<std::mutex> lock(wait_cv_mutex);
        wait_cv.notify_all();
    }
}

bool StorageMaterializedView::PipelineState::isCancelled() const noexcept
{
    if (is_cancelled.test())
        return true;

    if (auto io_ = io.load(); io_ && io_->process_list_entry)
        return io_->process_list_entry->getQueryStatus()->isKilled();

    return false;
}

bool StorageMaterializedView::PipelineState::pipelineStopped() const noexcept
{
    auto current_status = status.load();
    return current_status == Status::None || current_status == Status::Error || current_status == Status::Paused;
}

bool StorageMaterializedView::PipelineState::checkStatus(StorageMaterializedView::PipelineState::Status expected) const noexcept
{
    return status.load() == expected;
}

void StorageMaterializedView::PipelineState::updateStatus(StorageMaterializedView::PipelineState::Status status_) noexcept
{
    status.store(status_);
    status.notify_all();
}

bool StorageMaterializedView::PipelineState::compareAndUpdateStatus(
    StorageMaterializedView::PipelineState::Status & old_status, StorageMaterializedView::PipelineState::Status new_status) noexcept
{
    bool updated = status.compare_exchange_strong(old_status, new_status);
    if (updated)
        status.notify_all();

    return updated;
}

void StorageMaterializedView::PipelineState::waitStatusChanged(StorageMaterializedView::PipelineState::Status old_status) const
{
    auto current_status = status.load(std::memory_order_relaxed);
    while (current_status == old_status)
    {
        if (is_cancelled.test())
            return;

        status.wait(old_status);
        current_status = status.load(std::memory_order_relaxed);
    }
}

bool StorageMaterializedView::PipelineState::waitPipelineFinished(size_t max_wait_ms) const
{
    auto check_interval = std::min<size_t>(10, max_wait_ms);
    Stopwatch stopwatch;
    while (active())
    {
        if (max_wait_ms > 0 && stopwatch.elapsedMilliseconds() >= max_wait_ms)
            return false; /// TIMEOUT

        waitFor(check_interval);
    }
    return true;
}

void StorageMaterializedView::PipelineState::waitFor(size_t wait_ms) const
{
    /// No care of spurious awakenings
    std::unique_lock lock(wait_cv_mutex);
    wait_cv.wait_for(lock, std::chrono::milliseconds(wait_ms));
}

void StorageMaterializedView::PipelineState::setException(int code, String msg)
{
    std::scoped_lock lock{last_err_msg_mutex};

    last_err_msg.swap(msg);
    last_err_msg_ts = UTCMilliseconds::now();
    err = code;
}

void StorageMaterializedView::PipelineState::checkException(const String & msg_prefix) const
{
    auto err_code = err.load();
    if (err_code != ErrorCodes::OK)
        throw Exception(err_code, "{} {}", msg_prefix, last_err_msg);
}

void StorageMaterializedView::PipelineState::resetPipeline()
{
    /// Save the last streaming source metrics before resetting the pipeline, this is used to getStreamingSourceMetrics() later
    boost::shared_ptr<BlockIO> empty_io;
    if (const auto block_io = io.exchange(empty_io); block_io)
    {
        auto sources_metrics = boost::make_shared<Streaming::StreamingSourceMetricsPtrs>();
        for (const auto & ss : block_io->pipeline.getStreamingSources())
            sources_metrics->emplace_back(ss->getMetrics());

        last_streaming_source_metrics.store(std::move(sources_metrics));
    }

    /// Reset CPU tracking to avoid misleading spikes after pause/resume
    last_cpu_microseconds.store(0, std::memory_order_relaxed);
    last_elapsed_microseconds.store(0, std::memory_order_relaxed);

    /// Clear cached static pipeline data
    cached_thread_ids.clear();
}

bool StorageMaterializedView::initPipeline(bool recovering)
{
    std::lock_guard lock{pipeline_state_mutex};
    if (pipeline_state.active())
    {
        if (pipeline_state.executor.joinable())
            LOG_INFO(logger, "Skip initializing background state: pipeline thread is still running, status={}", getPipelineStatus());
        else
            LOG_WARNING(
                logger, "Skip initializing background state: pipeline thread (detached) is still running, status={}", getPipelineStatus());
        return false;
    }

    /// The thread has exited but not joined yet
    if (pipeline_state.executor.joinable())
        pipeline_state.executor.join();

    /// Reset the pipeline state after recovery
    pipeline_state.resetPipeline();
    pipeline_state.query_context = nullptr;
    pipeline_state.retry_times = 0;
    pipeline_state.err = ErrorCodes::OK;
    pipeline_state.is_cancelled.clear();
    pipeline_state.failed_err_with_count = {ErrorCodes::OK, 0};
    pipeline_state.recover_sns.clear();
    /// For create request, need validate the pipeline, so if the pipeline failed before executing, it will update status to `FATAL`,
    /// and wake up the blocked thread (startup()) and then check whether exception exists later
    pipeline_state.validated.store(recovering);
    pipeline_state.validated.notify_all();

    pipeline_state.exec_mode = recovering ? ExecuteMode::Recover : ExecuteMode::Subscribe;
    pipeline_state.updateStatus(recovering ? PipelineState::Status::Recovering : PipelineState::Status::Initializing);

    /// Ensure \mv_holder remains in scope while the background pipeline thread is running, since
    /// it may be detached and continue execution in the background.
    pipeline_state.executor = ThreadFromGlobalPool{[mv_holder = shared_from_this(), this]() noexcept { runPipeline(); }};
    return true;
}

void StorageMaterializedView::shutdownPipeline()
{
    std::lock_guard lock{pipeline_state_mutex};
    pipeline_state.shutdown();
}

void StorageMaterializedView::runPipeline() noexcept
{
    setThreadName("MVBgQueryEx");

    pipeline_state.executor_tid.store(getThreadId());

    std::string_view qid = getInnerQueryId();
    auto current_status = getPipelineStatus();

    SCOPE_EXIT_SAFE({
        pipeline_state.resetPipeline();
        pipeline_state.query_context.reset();

        current_status = getPipelineStatus();
        chassert(current_status == PipelineState::Status::None || current_status == PipelineState::Status::Error);

        pipeline_state.cancel(); /// enforce set cancelled when fatal occurred

        LOG_INFO(logger, "Stopped background pipeline thread with status '{}' for query_id={}", current_status, qid);

        pipeline_state.executor_tid.store(0); /// Reset the executor_tid after thread exit
    });

    try
    {
        LOG_INFO(logger, "Started background pipeline thread with status '{}' for query_id={}", current_status, qid);

        pipeline_state.query_context = Context::createCopy(getContext());
        syncInnerQueryContext(pipeline_state.query_context);
        CurrentThread::QueryScope query_scope(pipeline_state.query_context);

        pipeline_state.pause_on_start = pipeline_state.query_context->getSettingsRef().pause_on_start.value;

        /// Pipeline Status Transition:
        /// Normal case: Initializing -> CheckingDependencies -> BuildingPipeline -> ExecutingPipeline => (Exit) None
        /// Other cases:
        ///   a. AutoRecovery: (XXX) -> AutoRecovering      => (i. retriable)    Initialized
        ///                                                 => (ii. unretriable) Error
        ///
        ///
        ///   b. SYSTEM pause:      (XXX)   -> Paused
        ///      SYSTEM resume:     Paused  -> Resuming -> (resumed) Initialized
        ///      SYSTEM abort:      (XXX)   -> Error
        ///      SYSTEM recover:    Error   -> Recovering -> (recovered) Initialized
        /// ...

        while (!pipeline_state.isCancelled())
        {
            try
            {
                current_status = getPipelineStatus();
                switch (current_status)
                {
                    case PipelineState::Status::None:
                    {
                        return; /// Exit normally
                    }
                    case PipelineState::Status::Initializing:
                    {
                        initialize();
                        break;
                    }
                    case PipelineState::Status::Checking:
                    {
                        checkDependencies();
                        break;
                    }
                    case PipelineState::Status::Building:
                    {
                        buildBackgroundPipeline();
                        break;
                    }
                    case PipelineState::Status::Executing:
                    {
                        executeBackgroundPipeline();
                        break;
                    }
                    case PipelineState::Status::Error:
                    {
                        LOG_ERROR(logger, "Aborted pipeline for query_id={}", qid);
                        return;
                    }
                    case PipelineState::Status::Paused:
                    {
                        LOG_INFO(logger, "Paused pipeline for query_id={}", qid);
                        pipeline_state.resetPipeline(); /// Release the QueryPipeline and deregister from ProcessList
                        pipeline_state.waitStatusChanged(current_status); /// Wait until resumed or canceled
                        break;
                    }
                    case PipelineState::Status::Resuming:
                    {
                        LOG_INFO(logger, "Resuming pipeline for query_id={}", qid);
                        pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Initializing);
                        break;
                    }
                    case PipelineState::Status::Recovering:
                    {
                        LOG_INFO(logger, "Recovering pipeline for query_id={}", qid);
                        pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Initializing);
                        break;
                    }
                    case PipelineState::Status::AutoRecovering:
                    {
                        prepareRecoveryAfterException();
                        break;
                    }
                }
            }
            catch (const DB::Exception & e)
            {
                /// While each step should ensure its own state transition, running on any step may still throw a memory exceeded exception, so we have to catch it
                if (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                {
                    /// Memory limit exceeded is a fatal error
                    pipeline_state.setException(e.code(), getExceptionMessage(e, true));
                    pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::AutoRecovering);
                }
                else
                {
                    e.rethrow();
                }
            }
        }
    }
    catch (const Exception & e)
    {
        /// Not log error for internal cancelled
        if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED && pipeline_state.isCancelled())
        {
            pipeline_state.updateStatus(PipelineState::Status::None);
            return;
        }

        tryLogCurrentException(
            logger,
            fmt::format(
                "Unretriable failure occurred in background pipeline thread (retried {} times), will exit the thread",
                pipeline_state.retry_times.load(std::memory_order_relaxed)));

        pipeline_state.setException(e.code(), e.message());
        pipeline_state.updateStatus(PipelineState::Status::Error);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Unknown failure occurred in background pipeline thread, will exit the thread");
        pipeline_state.setException(getCurrentExceptionCode(), getCurrentExceptionMessage(false));
        pipeline_state.updateStatus(PipelineState::Status::Error);
    }
}

void StorageMaterializedView::initialize()
{
    auto current_status = PipelineState::Status::Initializing;
    LOG_INFO(logger, "Initializing for internal query_id={}", getInnerQueryId());

    pipeline_state.err = ErrorCodes::OK;

    syncInnerQueryContext(pipeline_state.query_context);

    prepareCheckpoint();

    handleDLQSettings();

    /// If already validated, we shall get stillness status in below scenarios:
    if (pipeline_state.validated.load())
    {
        if (pipeline_state.pause_on_start)
        {
            pipeline_state.updateStatus(PipelineState::Status::Paused);
            pipeline_state.pause_on_start = false;

            return;
        }
    }

    pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Checking);
}

void StorageMaterializedView::checkDependencies()
{
    auto current_status = PipelineState::Status::Checking;
    /// During server bootstrap, we shall startup all tables in parallel, so here
    /// needs validity check underlying tables.
    LOG_INFO(logger, "Checking dependencies for query_id={}", getInnerQueryId());

    std::list<StoragePtr> waiting_storages;

    /// Check target stream
    if (auto target = getTargetTable(); !target->isReady())
        waiting_storages.emplace_back(std::move(target));

    /// Check source storages
    auto context = getContext();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
    {
        if (unlikely(select_table_id == getStorageID()))
            continue;

        auto source_storage = DatabaseCatalog::instance().getTable(select_table_id, context);
        if (!source_storage->isReady())
            waiting_storages.emplace_back(std::move(source_storage));
    }

    if (!waiting_storages.empty())
    {
        auto waiting_storages_names_v
            = waiting_storages | std::views::transform([](const auto & storage) { return storage->getStorageID().getFullTableName(); });

        /// For create request, don't wait for dependencies to be ready
        if (!pipeline_state.validated.load())
        {
            pipeline_state.setException(
                ErrorCodes::RESOURCE_NOT_INITED,
                fmt::format("The dependencies '{}' are not ready yet", fmt::join(waiting_storages_names_v, ", ")));
            pipeline_state.updateStatus(PipelineState::Status::Error);
            return;
        }

        /// Wait for dependencies to be ready
        size_t current_check_times = 1;
        do
        {
            /// Log error every 50 times (5s)
            if (current_check_times % 50 == 0)
                LOG_ERROR(
                    logger,
                    "The dependencies '{}' are not ready yet, checked {} times",
                    fmt::join(waiting_storages_names_v, ", "),
                    current_check_times);

            pipeline_state.waitFor(internal_recheck_interval_ms);
            if (pipeline_state.isCancelled() || !pipeline_state.checkStatus(current_status))
                return;

            std::erase_if(waiting_storages, [](const auto & storage) { return storage->isReady(); });
            ++current_check_times;
        } while (!waiting_storages.empty());
    }

    /// Next status
    pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Building);
}

void StorageMaterializedView::buildBackgroundPipeline()
{
    auto current_status = PipelineState::Status::Building;
    LOG_INFO(logger, "Building pipeline for query_id={}", getInnerQueryId());
    try
    {
        pipeline_state.resetPipeline();

        auto target_table = getTargetTable();
        auto metadata_snapshot = getInMemoryMetadataPtr();

        if (pipeline_state.exec_mode != ExecuteMode::Normal)
            pipeline_state.query_context->setSetting(
                "exec_mode", pipeline_state.exec_mode == ExecuteMode::Recover ? String("recover") : String("subscribe"));

        pipeline_state.query_context->setCurrentQueryId(getInnerQueryId()); /// Use a query_id bound to the uuid of mv
        pipeline_state.query_context->setInternalQuery(false); /// We like to log materialized query like a regular one
        pipeline_state.query_context->setInsertionTable(target_table->getStorageID());
        pipeline_state.query_context->setIngestMode(IngestMode::Sync); /// Sync ingest to make sure the sink is successful
        pipeline_state.query_context->setSetting("insert_max_tries", 100);
        pipeline_state.query_context->setCreator(getStorageID().getFullNameNotQuoted());
        pipeline_state.query_context->setQueryFromMaterializedView(true);

        auto inner_query = metadata_snapshot->getSelectQuery().inner_query->clone();
        auto io = boost::make_shared<BlockIO>();
        io->process_list_entry = pipeline_state.query_context->getProcessList().insert(
            serializeAST(*inner_query), inner_query.get(), pipeline_state.query_context, Stopwatch{CLOCK_MONOTONIC}.getStart());
        pipeline_state.query_context->setProcessListElement(io->process_list_entry->getQueryStatus());
        pipeline_state.query_context->setProgressCallback([&](const Progress & progress) {
            const auto & value = progress.getValues();
            pipeline_state.read_bytes.fetch_add(value.read_bytes, std::memory_order_relaxed);
            pipeline_state.read_rows.fetch_add(value.read_rows, std::memory_order_relaxed);
        });
        pipeline_state.query_context->setWriteProgressCallback([&](const Progress & progress) {
            const auto & value = progress.getValues();
            pipeline_state.written_bytes.fetch_add(value.written_bytes, std::memory_order_relaxed);
            pipeline_state.written_rows.fetch_add(value.written_rows, std::memory_order_relaxed);
        });
        CurrentThread::get().performance_counters[ProfileEvents::OSCPUWaitMicroseconds] = 0;
        CurrentThread::get().performance_counters[ProfileEvents::OSCPUVirtualTimeMicroseconds] = 0;

        io->pipeline = buildQueryPipelineImpl(inner_query, pipeline_state.query_context, &pipeline_state.can_recover_without_checkpoint);

        /// Restore the previous error message to new processor metrics for pipeline query
        const auto & new_processors = io->pipeline.getProcessors();
        if (new_processors.size() == pipeline_state.processors_last_error_message.size())
        {
            for (size_t i = 0; i < new_processors.size(); ++i)
                if (const auto & m = pipeline_state.processors_last_error_message[i]; !m.empty())
                    new_processors.at(i)->setLastErrorMessage(m);
        }

        /// Other threads may call `shutdown` to cancel the pipeline execution (via `process_list_entry->cancelQuery(true)`)
        chassert(io && io->process_list_entry && io->pipeline.initialized());

        /// Collect static pipeline data once (thread IDs)
        /// These don't change during pipeline execution, so cache them to avoid repeated collection
        if (io->process_list_entry)
        {
            auto static_info = io->process_list_entry->getQueryStatus()->getInfo(
                /*get_thread_list=*/true, /*get_profile_events=*/false, /*get_settings=*/false);
            pipeline_state.cached_thread_ids = std::move(static_info.thread_ids);
        }

        pipeline_state.io.store(io);
        pipeline_state.validated.store(true);
        pipeline_state.validated.notify_all();

        /// Next status
        /// - If the scheduled mv is not scheduled, then exit now
        if (pipeline_state.pause_on_start)
        {
            pipeline_state.updateStatus(PipelineState::Status::Paused);
            pipeline_state.pause_on_start = false;
        }
        else
        {
            pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Executing);
        }
    }
    catch (...)
    {
        pipeline_state.setException(getCurrentExceptionCode(), getCurrentExceptionMessage(true));

        /// For create request, we need validate the pipeline, so if the pipeline failed before executing, it will update status to `FATAL` and wake up the blocked thread (startup())
        if (!pipeline_state.validated.load())
            pipeline_state.updateStatus(PipelineState::Status::Error);
        else
            pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::AutoRecovering);
    }
}

QueryPipeline StorageMaterializedView::buildQueryPipelineImpl(
    ASTPtr inner_query, ContextMutablePtr query_context, bool * can_recover_without_checkpoint) const
{
    auto target_table = getTargetTable();
    auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    /// NOTE: Here we build two stages `select` + `insert` instead of directly building `insert select`,
    /// so that the pipeline can be adjusted more flexibly
    QueryPipelineBuilder pipeline_builder;
    {
        InterpreterSelectWithUnionQuery select_interpreter(inner_query, query_context, SelectQueryOptions());

        /// TODO: In some cases (\can_recover_without_checkpoint = true), if checkpointing is too expensive, consider disabling it.
        if (can_recover_without_checkpoint)
            *can_recover_without_checkpoint = select_interpreter.isConsistentWithoutCheckpoint();

        /// [Pipeline]: `Source` -> `Converting` -> `Materializing const` -> `target_table`
        pipeline_builder = select_interpreter.buildQueryPipeline();
        pipeline_builder.dropTotalsAndExtremes();
    }

    const auto & source_header = pipeline_builder.getHeader();

    Block target_header;
    Names insert_columns;
    target_header.reserve(source_header.columns());
    insert_columns.reserve(source_header.columns());
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
        else
        {
            LOG_WARNING(
                logger,
                "Column '{}' in select output is not found in {} table '{}', will be skipped",
                source_column.name,
                target_table_id ? "target" : "inner",
                target_table->getStorageID().getFullTableName());
        }
    }

    if (target_header.columns() == 0)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "No matching columns found between select outputs and target table '{}', select output: [{}], target header: [{}]",
            target_table->getStorageID().getFullTableName(),
            source_header.dumpNames(),
            target_storage_header.dumpNames());

    size_t out_streams_size = 1;
    const auto & settings = query_context->getSettingsRef();
    if (target_table->supportsParallelInsert() && settings.max_insert_threads > 1)
        out_streams_size = std::min<size_t>(settings.max_insert_threads, pipeline_builder.getNumStreams());

    pipeline_builder.resize(out_streams_size);

    if (!blocksHaveEqualStructure(source_header, target_header))
    {
        /// Use match by name since the `select` output header may not match the target stream's schema
        auto converting = ActionsDAG::makeConvertingActions(
            source_header.getColumnsWithTypeAndName(), target_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(converting), ExpressionActionsSettings::fromContext(query_context, CompileExpressions::yes));

        pipeline_builder.addSimpleTransform(
            [&](const Block & header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(header, actions); });
    }

    /// Materializing const columns
    pipeline_builder.addSimpleTransform(
        [&](const Block & header) -> ProcessorPtr { return std::make_shared<MaterializingTransform>(header); });

    /// Sink to target table
    std::vector<Chain> out_chains;
    out_chains.reserve(out_streams_size);
    for (size_t i = 0; i < out_streams_size; ++i)
    {
        InterpreterInsertQuery interpreter(nullptr, query_context, false, false, false);
        /// FIXME, thread status
        auto out_chain = interpreter.buildChain(
            target_table, target_metadata_snapshot, insert_columns, nullptr, nullptr, pipeline_builder.isStreaming());
        out_chains.emplace_back(std::move(out_chain));
    }

    QueryPlanResourceHolder resources;
    for (auto & out_chain : out_chains)
        resources = out_chain.detachResources();

    pipeline_builder.addChains(std::move(out_chains));

    pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<EmptySink>(cur_header);
    });

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    pipeline.addResources(std::move(resources));
    pipeline.addStorageHolder(target_table);
    pipeline.setProgressCallback(query_context->getProgressCallback());
    pipeline.setProcessListElement(query_context->getProcessListElement());
    return pipeline;
}

void StorageMaterializedView::executeBackgroundPipeline()
{
    auto current_status = PipelineState::Status::Executing;

    LOG_INFO(logger, "Executing pipeline on mode '{}' for query_id={}", pipeline_state.exec_mode, getInnerQueryId());

    try
    {
        auto io = pipeline_state.io.load();
        chassert(io);

        if (!pipeline_state.recover_sns.empty())
        {
            resetSNForStreamingSources(io->pipeline.getStreamingSources(), pipeline_state.recover_sns);
            pipeline_state.recover_sns.clear();
        }

        io->pipeline.setExecuteMode(pipeline_state.exec_mode);
        CompletedPipelineExecutor executor(io->pipeline);

        executor.registerCheckpoint(curr_ckpt_ctx);

        bool status_changed = false;
        {
            executor.setCancelCallback(
                [&status_changed, this] {
                    status_changed |= !pipeline_state.checkStatus(PipelineState::Status::Executing);
                    return status_changed;
                },
                internal_recheck_interval_ms);
        }

        if (pipeline_state.isCancelled())
        {
            LOG_WARNING(logger, "The background pipeline is killed before execution, it's abnormal");
            return;
        }

        executor.execute();

        if (status_changed)
        {
            LOG_WARNING(
                logger, "Interrupt pipeline execution: the pipeline status changed externally, current status: {}", getPipelineStatus());
            return;
        }

        /// If the pipeline is finished which is not caused by a cancel, there is something wrong since we expect the background streaming query
        /// to run forever until an explicit cancel or checkpoint leader switch; other than these something wrong probably happened.
        /// An example : a background query likes stream join table, and the table has no data, the background pipeline will be finished immediately.
        /// In this case, we need throw a exception to notify users and retry it.
        pipeline_state.setException(
            ErrorCodes::PIPELINE_FINISHED_EARLY,
            "Streaming Pipeline finished early. This usually happens when joined tables are empty and the streaming pipeline exists");
        pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::AutoRecovering);
    }
    catch (...)
    {
        /// Always try to recover the pipeline
        pipeline_state.setException(getCurrentExceptionCode(), getCurrentExceptionMessage(true));
        pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::AutoRecovering);
    }
}

void StorageMaterializedView::prepareRecoveryAfterException()
{
    auto current_status = PipelineState::Status::AutoRecovering;

    auto err_code = pipeline_state.err.load();
    auto [err_msg, err_ts] = pipeline_state.lastErrorMessageAndTimestamp();
    auto retry_times = pipeline_state.retry_times.fetch_add(1, std::memory_order_relaxed);

    chassert(err_code != ErrorCodes::OK);

    /// If the pipeline is cancelled, we can exit the thread immediately before handing the exception
    if (pipeline_state.isCancelled() || err_code == ErrorCodes::MATERIALIZED_VIEW_STOPPED)
    {
        /// Cancel is treated like normal, when we loop over, we will exit the thread
        pipeline_state.err = ErrorCodes::OK;
        return;
    }

    if (err_code == ErrorCodes::QUERY_WAS_CANCELLED)
    {
        /// Query was cancelled, we can exit the thread immediately
        pipeline_state.err = ErrorCodes::OK;
        pipeline_state.cancel(/*abort=*/false);
        return;
    }

    /// Pipeline Status Transition:
    /// AutoRecovering  ==> (retriable)    => Initialized
    ///                 |   (unretriable)  => Error
    ///                 v
    ///          (SYSTEM PAUSE) => Paused
    ///          (SYSTEM ABORT) => Error
    /// @t1: a. Usually, we will update the status to 'Recovering' and then start handling the error and check it is retry-able or not.
    ///      b. Specifically, other thread execute system command to pause or abort the mv.
    /// @t2: a. Usually, if the error is retry-able, we will update the status to 'Initializing' and then retry to recover the pipeline.
    ///         otherwise, we will update the status to 'Error' and then exit the thread.
    ///      b. Specifically, other thread execute system command to pause or abort the mv.
    LOG_ERROR(logger, "Background runtime error: {}", err_msg);

    std::vector<std::shared_ptr<Streaming::ISource>> sources;
    StreamingSourcesDescription streaming_sources_description;
    if (auto io = pipeline_state.io.load())
    {
        sources = io->pipeline.getStreamingSources();
        streaming_sources_description = getStreamingSourcesDescription(sources);

        /// Backup the processor last error message for later restore in the recovered pipeline
        const auto & processors = io->pipeline.getProcessors();
        pipeline_state.processors_last_error_message.resize(processors.size());
        for (size_t i = 0; i < processors.size(); ++i)
            pipeline_state.processors_last_error_message[i] = std::move(processors[i]->getMetrics().last_error_message);

        pipeline_state.resetPipeline();
    }

    auto prepare_recovery = [&]() {
        if (pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Initializing))
            LOG_INFO(
                logger,
                "{}th recovering pipeline {}",
                retry_times + 1,
                streaming_sources_description.dumpWithSNs(pipeline_state.recover_sns));
        else
            LOG_WARNING(logger, "Interrupt pipeline auto-recovery, since the pipeline status has been changed to '{}'", current_status);
    };

    /// FIXME: Add more (un)retriable error handling
    if (err_code == ErrorCodes::CHECKPOINT_DOESNT_EXIST)
    {
        LOG_WARNING(logger, "Re-subscribe pipeline since the checkpoint doesn't exist");
        if (curr_ckpt_ctx)
            Globals::getCheckpointCoordinator().removeCheckpoint(curr_ckpt_ctx, /*sync=*/true);
        return prepare_recovery();
    }

    if (isPermanentError(err_code))
    {
        /// If checkpoints aren’t required for recovery, remove the invalid checkpoint and restart recovery.
        if (err_code == ErrorCodes::RECOVER_CHECKPOINT_FAILED && pipeline_state.can_recover_without_checkpoint)
        {
            LOG_WARNING(logger, "Re-subscribe pipeline after removing the invalid checkpoint");
            if (curr_ckpt_ctx)
                Globals::getCheckpointCoordinator().removeCheckpoint(curr_ckpt_ctx, /*sync=*/true);
            return prepare_recovery();
        }

        pipeline_state.compareAndUpdateStatus(current_status, PipelineState::Status::Error);
        return;
    }

    if (err_code == ErrorCodes::PIPELINE_FINISHED_EARLY || err_code == ErrorCodes::INGEST_TIMEOUT || err_code == ErrorCodes::SOCKET_TIMEOUT
        || err_code == ErrorCodes::NETWORK_ERROR || err_code == ErrorCodes::CANNOT_CONNECT_SERVER)
    {
        pipeline_state.waitFor(recover_backoff_ms.backoff(retry_times));
        return prepare_recovery();
    }

    /// For other errors, we will handle it by recovery_policy
    const auto & settings = pipeline_state.query_context->getSettingsRef();
    switch (settings.recovery_policy)
    {
        case RecoveryPolicy::Strict:
        {
            /// For some queries that always go wrong, we don’t want to recover too frequently.
            pipeline_state.waitFor(recover_backoff_ms.backoff(retry_times));
            return prepare_recovery();
        }
        case RecoveryPolicy::BestEffort:
        {
            const auto & current_failed_sns_range = streaming_sources_description.processed_sns_range;
            /// 1) Try skip poison data:
            ///   a. If current error is known poison events error, we will skip the poison data and recover from the next SN
            bool skipped_poison_data = false;
            if (!current_failed_sns_range.empty() && KNOWN_POISON_EVENTS_ERRORS.contains(err_code))
            {
                if (pipeline_state.continuously_dropped_sn_ranges.empty())
                    pipeline_state.continuously_dropped_sn_ranges = std::vector<Streaming::SequenceRange>{
                        current_failed_sns_range.size(), Streaming::SequenceRange{.start = 0, .end = 0}};

                std::vector<Int64> next_recover_sns;
                next_recover_sns.reserve(current_failed_sns_range.size());
                for (size_t i = 0; i < current_failed_sns_range.size(); ++i)
                {
                    auto failed_start_sn = current_failed_sns_range[i].start;
                    if (failed_start_sn >= cluster::Constants::LogStartSN)
                    {
                        skipped_poison_data = true;
                        const auto & poison_sn_range = current_failed_sns_range[i];
                        LOG_WARNING(
                            logger,
                            "Skip the poison data (sn_range={}-{}) for {}({})",
                            poison_sn_range.start,
                            poison_sn_range.end,
                            streaming_sources_description.names[i],
                            streaming_sources_description.descriptions[i]);
                        next_recover_sns.emplace_back(poison_sn_range.end + 1);

                        /// If the ranges are not continuous, reset it
                        if (pipeline_state.continuously_dropped_sn_ranges[i].end + 1 != poison_sn_range.start)
                            pipeline_state.continuously_dropped_sn_ranges[i].start = poison_sn_range.start;
                        pipeline_state.continuously_dropped_sn_ranges[i].end = poison_sn_range.end;
                    }
                    else
                    {
                        next_recover_sns.emplace_back(-1); /// Invalid SN, indicates still recover from checkpointed
                    }
                }

                /// All are invalid SNs means we need recover from checkpointed
                if (std::ranges::all_of(next_recover_sns, [](auto sn) { return sn < 0; }))
                    next_recover_sns.clear();

                pipeline_state.recover_sns.swap(next_recover_sns);
            }

            /// 2) If same error occurs >=8 times in a row, we will abort the background pipeline execution
            /// NOTE: In general, we can continue to do recovery even if poison data appears many times, so reset 1)'s state
            if (skipped_poison_data)
            {
                if (auto pause_until_ts = pipeline_state.pause_dlq_until; pause_until_ts > 0)
                {
                    if (MonotonicSeconds::now() >= pause_until_ts)
                    {
                        /// Cleanup, or it will immediately pause again after unpause.
                        pipeline_state.continuously_dropped_sn_ranges = {};
                        pipeline_state.pause_dlq_until = 0;
                    }
                }
                else if (pipeline_state.enable_dlq)
                {
                    /// If a source is continously dropping data, that might be an indicator tells that there is something
                    /// wrong with the MV, maybe it's the schema, maybe it's the query. Then stop sending data to DLQ for
                    /// some time (for now, hard-coded 1 minute).
                    auto drop_limit = pipeline_state.dlq_consecutive_failures_limit.load();
                    if (drop_limit != 0 /// unlimited
                        && std::ranges::any_of(pipeline_state.continuously_dropped_sn_ranges, [drop_limit](const auto & sn_range) {
                               return static_cast<uint64_t>(sn_range.count()) > drop_limit;
                           }))
                    {
                        LOG_ERROR(
                            logger,
                            "The MV has dropped more than {} records in a row, stop sending data to DLQ for the next 1 minute",
                            drop_limit);

                        pipeline_state.pause_dlq_until = MonotonicSeconds::now() + 60;
                        return;
                    }

                    logToDLQ(sources);
                }
                else
                {
                    pipeline_state.failed_err_with_count = {ErrorCodes::OK, 0};
                }
            }
            else if (pipeline_state.failed_err_with_count.first == err_code)
            {
                if (++pipeline_state.failed_err_with_count.second >= settings.recovery_retry_for_same_error)
                {
                    LOG_ERROR(
                        logger,
                        "Cannot recover pipeline, The same error '{}' has occurred more than {} times in a row. Need recover/recreate "
                        "manually",
                        ErrorCodes::getName(err_code),
                        settings.recovery_retry_for_same_error);
                    pipeline_state.updateStatus(PipelineState::Status::Error);
                    return;
                }
            }
            else
            {
                pipeline_state.failed_err_with_count = {err_code, 1};
            }

            if (pipeline_state.failed_err_with_count.second > 0)
                pipeline_state.waitFor(recover_backoff_ms.backoff(pipeline_state.failed_err_with_count.second));

            return prepare_recovery();
        }
    }

    UNREACHABLE();
}

int StorageMaterializedView::flushAndStop(PipelineState::Status to_status, int64_t timeout_ms)
{
    try
    {
        /// So far, only used for stopping, pausing and restarting pipeline
        chassert(
            to_status == PipelineState::Status::None || to_status == PipelineState::Status::Paused
            || to_status == PipelineState::Status::Initializing);

        Stopwatch stopwatch;
        auto current_status = getPipelineStatus();
        auto update_status = [&]() {
            bool updated = pipeline_state.compareAndUpdateStatus(current_status, to_status);
            if (updated || current_status == to_status)
            {
                return ErrorCodes::OK;
            }
            else
            {
                LOG_INFO(
                    logger, "Skip stopping background pipeline since the status has already changed, current status: {}", current_status);
                return ErrorCodes::MATERIALIZED_VIEW_STATUS_CHANGED;
            }
        };

        if (pipeline_state.isCancelled())
        {
            if (to_status == PipelineState::Status::None || to_status == PipelineState::Status::Error)
                return update_status();
            else
                return ErrorCodes::MATERIALIZED_VIEW_STOPPED;
        }

        /// Trigger a checkpoint before stopping the pipeline
        if (current_status != PipelineState::Status::Executing)
            return update_status();

        std::shared_ptr<Poco::Event> event = std::make_shared<Poco::Event>();
        auto checkpointed_callback = [mv_holder = shared_from_this(), to_status, event, this](CheckpointContextPtr) {
            /// Already finished last checkpoint, we can stop the background pipeline now:
            /// 1) set checkpointed event
            /// 2) update status to 'to_status'
            /// 3) interrupt current pipeline execution
            event->set();
            pipeline_state.updateStatus(to_status);
            throw Exception(ErrorCodes::MATERIALIZED_VIEW_STOPPED, "Stopped background pipeline");
        };

        auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
        std::shared_ptr<Poco::Event> triggered_event;
        while (true)
        {
            if (!pipeline_state.checkStatus(PipelineState::Status::Executing))
                return update_status();

            if (timeout_ms > 0 && stopwatch.elapsedMilliseconds() >= static_cast<UInt64>(timeout_ms))
                return ErrorCodes::TIMEOUT_EXCEEDED;

            if (!triggered_event)
            {
                auto res = ckpt_coordinator.triggerCheckpointForQuery(getInnerQueryId(), checkpointed_callback);
                if (res == CheckpointCoordinator::TriggerResult::Success)
                    triggered_event = event;
            }

            if (triggered_event)
            {
                if (triggered_event->tryWait(internal_recheck_interval_ms))
                    return update_status();
            }
            else
            {
                /// If checkpointing is disabled, we can stop it directly
                if (pipeline_state.disable_checkpoint.load() || !ckpt_coordinator.isRegistered(getInnerQueryId()))
                    return update_status();

                pipeline_state.waitFor(internal_recheck_interval_ms);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to flush and stop background pipeline");
        return getCurrentExceptionCode();
    }
}

int StorageMaterializedView::pause(int64_t timeout_ms)
{
    return flushAndStop(PipelineState::Status::Paused, timeout_ms);
}

int StorageMaterializedView::resume()
{
    auto expected = PipelineState::Status::Paused;
    if (!pipeline_state.compareAndUpdateStatus(expected, PipelineState::Status::Resuming))
        LOG_INFO(logger, "Ignored resume command, current status: {}", expected);

    return ErrorCodes::OK;
}

int StorageMaterializedView::abort()
{
    try
    {
        shutdownPipeline();
        return ErrorCodes::OK;
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to abort background pipeline");
        return getCurrentExceptionCode();
    }
}

int StorageMaterializedView::recover()
{
    try
    {
        /// Reinit background state in recover mode
        bool inited = initPipeline(/*recovering=*/true);

        /// In case when the pipeline thread has not exited, return timeout directly, need to retry
        return inited ? ErrorCodes::OK : ErrorCodes::TIMEOUT_EXCEEDED;
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to recover background pipeline");
        return getCurrentExceptionCode();
    }
}

void StorageMaterializedView::syncInnerQueryContext(ContextMutablePtr query_context) const
{
    /// Sync settings from inner query
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & settings_changes = metadata_snapshot->getSelectQuery().settings_changes;
    query_context->checkSettingsConstraints(settings_changes);
    query_context->applySettingsChanges(settings_changes);
}

void StorageMaterializedView::logToDLQ(const std::vector<std::shared_ptr<Streaming::ISource>> & sources)
{
    if (sources.empty())
        return;

    if (!pipeline_state.enable_dlq.load())
        return;

    const auto & dlq = pipeline_state.query_context->getMaterializedViewDLQ();
    if (!dlq)
        return;

    auto [err_msg, err_ts] = pipeline_state.lastErrorMessageAndTimestamp();

    auto streaming_sources_description = getStreamingSourcesDescription(sources);

    MaterializedViewDLQElement dlq_ele{
        .node_id = pipeline_state.query_context->getNodeID(),
        .database = pipeline_state.mv.getStorageID().getDatabaseName(),
        .view_name = pipeline_state.mv.getStorageID().getTableName(),
        .uuid = pipeline_state.mv.getStorageID().uuid,
        .error = err_msg,
        .dead_letters = {},
        ._tp_time = DecimalField<DateTime64>(err_ts, 3),
    };

    const auto & current_failed_sns_range = streaming_sources_description.processed_sns_range;

    auto & letters = dlq_ele.dead_letters;
    letters.reserve(current_failed_sns_range.size());
    for (size_t i = 0; i < current_failed_sns_range.size(); ++i)
    {
        const auto & sn_range = current_failed_sns_range.at(i);
        Strings messages;
        if (auto limit = pipeline_state.dlq_max_message_batch_size.load(); static_cast<uint64_t>(sn_range.count()) > limit)
        {
            messages.push_back(fmt::format(
                "Problematic message range is too big which is over the default limit {}, raw messages fetch is skipped",
                sn_range.count(),
                limit));
        }
        else
        {
            try
            {
                messages = sources[i]->fetchData(sn_range);
            }
            catch (...)
            {
                /// In case of any kind of failure on fetching the data, we should record the error,
                /// so that the users can figure out what happened.
                messages.push_back(fmt::format("Failed to fetch original data: {}", getCurrentExceptionMessage(true)));
            }
        }
        letters.push_back(
            {.source_name = streaming_sources_description.names[i],
             .source_description = streaming_sources_description.descriptions[i],
             .sn_range = {sn_range.start, sn_range.end},
             .messages = messages});
    }
    dlq->add(dlq_ele);
}

}
