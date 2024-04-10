#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/ISource.h>
#include <QueryPipeline/printPipeline.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/setThreadName.h>

#ifndef NDEBUG
#    include <Common/Stopwatch.h>
#endif

/// proton: starts.
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Context.h>
#include <Interpreters/PipelineMetricLog.h>
#include <Common/VersionRevision.h>
/// proton: ends.


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
const String GRAPH_CKPT_FILE = "dag";
}

PipelineExecutor::PipelineExecutor(Processors & processors, QueryStatus * elem)
    : process_list_element(elem)
{
    if (process_list_element)
        profile_processors = process_list_element->getContext()->getSettingsRef().log_processors_profiles;

    try
    {
        graph = std::make_unique<ExecutingGraph>(processors, profile_processors);
    }
    catch (Exception & exception)
    {
        /// If exception was thrown while pipeline initialization, it means that query pipeline was not build correctly.
        /// It is logical error, and we need more information about pipeline.
        WriteBufferFromOwnString buf;
        printPipeline(processors, buf);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());

        throw;
    }

    if (process_list_element)
    {
        // Add the pipeline to the QueryStatus at the end to avoid issues if other things throw
        // as that would leave the executor "linked"
        process_list_element->addPipelineExecutor(this);
    }
}

PipelineExecutor::~PipelineExecutor()
{
    if (process_list_element)
        process_list_element->removePipelineExecutor(this);

    /// proton : starts
    deregisterCheckpoint();
    /// proton : ends
}

const Processors & PipelineExecutor::getProcessors() const
{
    return graph->getProcessors();
}

void PipelineExecutor::cancel()
{
    cancelled = true;

    /// proton : starts
    deregisterCheckpoint();
    /// proton : ends

    finish();
    graph->cancel();
}

void PipelineExecutor::finish()
{
    tasks.finish();
}

void PipelineExecutor::execute(size_t num_threads, ExecuteMode exec_mode_)
{
    checkTimeLimit();
    if (num_threads < 1)
        num_threads = 1;

    try
    {
        executeImpl(num_threads, exec_mode_);

        /// Execution can be stopped because of exception. Check and rethrow if any.
        for (auto & node : graph->nodes)
            if (node->exception)
                std::rethrow_exception(node->exception);

        /// Exception which happened in executing thread, but not at processor.
        tasks.rethrowFirstThreadException();
    }
    catch (...)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "Exception while executing query. Current state:\n{}", dumpPipeline());
#endif
        throw;
    }

    finalizeExecution();
}

bool PipelineExecutor::executeStep(std::atomic_bool * yield_flag)
{
    if (!is_execution_initialized)
    {
        initializeExecution(1);

        if (yield_flag && *yield_flag)
            return true;
    }

    executeStepImpl(0, yield_flag);

    if (!tasks.isFinished())
        return true;

    /// Execution can be stopped because of exception. Check and rethrow if any.
    for (auto & node : graph->nodes)
        if (node->exception)
            std::rethrow_exception(node->exception);

    finalizeExecution();

    return false;
}

bool PipelineExecutor::checkTimeLimitSoft()
{
    if (process_list_element)
    {
        bool continuing = process_list_element->checkTimeLimitSoft();
        // We call cancel here so that all processors are notified and tasks waken up
        // so that the "break" is faster and doesn't wait for long events
        if (!continuing)
            cancel();
        return continuing;
    }

    return true;
}

bool PipelineExecutor::checkTimeLimit()
{
    bool continuing = checkTimeLimitSoft();
    if (!continuing)
        process_list_element->checkTimeLimit(); // Will throw if needed

    return continuing;
}

void PipelineExecutor::setReadProgressCallback(ReadProgressCallbackPtr callback)
{
    read_progress_callback = std::move(callback);
}

void PipelineExecutor::finalizeExecution()
{
    checkTimeLimit();

    if (cancelled)
        return;

    /// proton: starts.
    if (process_list_element)
    {
        auto context_ptr = process_list_element->getContext();
        auto pipeline_metric_log = context_ptr->getPipelineMetricLog();

        if (pipeline_metric_log)
        {
            auto info = process_list_element->getInfo();

            PipelineMetricLogElement pipeline_metric_log_elem;
            auto finish_time = std::chrono::system_clock::now();

            pipeline_metric_log_elem.event_time = std::chrono::system_clock::to_time_t(finish_time);
            pipeline_metric_log_elem.event_time_microseconds = UTCMicroseconds::count(finish_time);
            pipeline_metric_log_elem.milliseconds = UTCMilliseconds::count(finish_time) - UTCSeconds::count(finish_time) * 1000;

            pipeline_metric_log_elem.query_id = info.client_info.current_query_id;
            pipeline_metric_log_elem.query = info.query;
            pipeline_metric_log_elem.pipeline_metric = info.pipeline_metrics;

            pipeline_metric_log->add(pipeline_metric_log_elem);
        }
    }
    /// proton: ends.

    bool all_processors_finished = true;
    for (auto & node : graph->nodes)
    {
        if (node->status != ExecutingGraph::ExecStatus::Finished)
        {
            /// Single thread, do not hold mutex
            all_processors_finished = false;
            break;
        }
    }

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::executeSingleThread(size_t thread_num)
{
    executeStepImpl(thread_num);

#ifndef NDEBUG
    auto & context = tasks.getThreadContext(thread_num);
    LOG_TRACE(log,
              "Thread finished. Total time: {} sec. Execution time: {} sec. Processing time: {} sec. Wait time: {} sec.",
              context.total_time_ns / 1e9,
              context.execution_time_ns / 1e9,
              context.processing_time_ns / 1e9,
              context.wait_time_ns / 1e9);
#endif
}

void PipelineExecutor::executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag)
{
#ifndef NDEBUG
    Stopwatch total_time_watch;
#endif

    // auto & node = tasks.getNode(thread_num);
    auto & context = tasks.getThreadContext(thread_num);
    bool yield = false;

    while (!tasks.isFinished() && !yield)
    {
        /// First, find any processor to execute.
        /// Just traverse graph and prepare any processor.
        while (!tasks.isFinished() && !context.hasTask())
            tasks.tryGetTask(context);

        while (context.hasTask() && !yield)
        {
            if (tasks.isFinished())
                break;

            if (!context.executeTask())
                cancel();

            if (tasks.isFinished())
                break;

            if (!checkTimeLimitSoft())
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                /// Prepare processor after execution.
                if (!graph->updateNode(context.getProcessorID(), queue, async_queue))
                    finish();

                /// Push other tasks to global queue.
                tasks.pushTasks(queue, async_queue, context);
            }

#ifndef NDEBUG
            context.processing_time_ns += processing_time_watch.elapsed();
#endif

            /// We have executed single processor. Check if we need to yield execution.
            if (yield_flag && *yield_flag)
                yield = true;
        }
    }

#ifndef NDEBUG
    context.total_time_ns += total_time_watch.elapsed();
    context.wait_time_ns = context.total_time_ns - context.execution_time_ns - context.processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    Queue queue;
    graph->initializeExecution(queue);

    tasks.init(num_threads, profile_processors, read_progress_callback.get());
    tasks.fill(queue);
}

void PipelineExecutor::executeImpl(size_t num_threads, ExecuteMode exec_mode_)
{
    OpenTelemetrySpanHolder span("PipelineExecutor::executeImpl()");

    initializeExecution(num_threads);

    /// proton : starts
    if (process_list_element)
        LOG_INFO(
            log, "Using {} threads to execute pipeline for query_id={}", num_threads, process_list_element->getClientInfo().current_query_id);

    execute_threads = num_threads;
    registerCheckpoint(exec_mode_);
    /// proton : ends

    using ThreadsData = std::vector<ThreadFromGlobalPool>;
    ThreadsData threads;
    threads.reserve(num_threads);

    bool finished_flag = false;

    SCOPE_EXIT_SAFE(
        if (!finished_flag)
        {
            finish();

            for (auto & thread : threads)
                if (thread.joinable())
                    thread.join();
        }
    );

    if (num_threads > 1)
    {
        auto thread_group = CurrentThread::getGroup();

        for (size_t i = 0; i < num_threads; ++i)
        {
            threads.emplace_back([this, thread_group, thread_num = i]
            {
                /// ThreadStatus thread_status;

                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                try
                {
                    executeSingleThread(thread_num);
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    tasks.getThreadContext(thread_num).setException(std::current_exception());
                }
            });
        }

        tasks.processAsyncTasks();

        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }
    else
        executeSingleThread(0);

    finished_flag = true;
}

String PipelineExecutor::dumpPipeline() const
{
    for (const auto & node : graph->nodes)
    {
        {
            WriteBufferFromOwnString buffer;
            buffer << "(" << node->num_executed_jobs << " jobs";

#ifndef NDEBUG
            buffer << ", execution time: " << node->execution_time_ns / 1e9 << " sec.";
            buffer << ", preparation time: " << node->preparation_time_ns / 1e9 << " sec.";
#endif

            buffer << ")";
            node->processor->setDescription(buffer.str());
        }
    }

    std::vector<IProcessor::Status> statuses;
    std::vector<IProcessor *> proc_list;
    statuses.reserve(graph->nodes.size());
    proc_list.reserve(graph->nodes.size());

    for (const auto & node : graph->nodes)
    {
        proc_list.emplace_back(node->processor);
        statuses.emplace_back(node->last_processor_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(graph->getProcessors(), statuses, out);
    out.finalize();

    return out.str();
}

/// proton: starts.
void PipelineExecutor::registerCheckpoint(ExecuteMode exec_mode_)
{
    if (exec_mode_ == ExecuteMode::NORMAL)
        return;

    assert(process_list_element);
    graph->initCheckpointNodes();

    auto query_context = process_list_element->getContext();

    Int64 interval = 0;
    if (exec_mode_ == ExecuteMode::RECOVER)
    {
        recover();
        interval = recovered_ckpt_interval;
    }
    else
        interval = query_context->getSettingsRef().checkpoint_interval.value;

    /// Here requires a lock to cover following scenarios in multi-threads:
    /// 1) registerQuery() -> dtor() OR cancel() -> deregisterQuery()
    /// 2) dtor() OR cancel() -> deregisterQuery() -> registerQuery()
    /// 3) dtor() OR cancel() -> registerQuery() -> deregisterQuery()
    std::lock_guard lock(register_checkpoint_mutex);
    if (cancelled)
        return;

    exec_mode = exec_mode_;
    auto & ckpt_coordinator = CheckpointCoordinator::instance(query_context->getGlobalContext());
    ckpt_coordinator.registerQuery(process_list_element->getClientInfo().current_query_id, process_list_element->getQuery(), interval, weak_from_this(), recovered_epoch);
}

void PipelineExecutor::deregisterCheckpoint()
{
    std::lock_guard lock(register_checkpoint_mutex);
    if (exec_mode == ExecuteMode::SUBSCRIBE || exec_mode == ExecuteMode::RECOVER)
    {
        CheckpointCoordinator::instance(process_list_element->getContext()->getGlobalContext())
            .deregisterQuery(process_list_element->getClientInfo().current_query_id);

        /// Reset exec_mode to NORMAL to avoid re-execute deregisterQuery again although it doesn't hurt
        exec_mode = ExecuteMode::NORMAL;
    }
}

void PipelineExecutor::serialize(CheckpointContextPtr ckpt_ctx) const
{
    if (exec_mode == ExecuteMode::SUBSCRIBE)
    {
        ckpt_ctx->coordinator->preCheckpoint(ckpt_ctx);

        /// For recover mode, we don't want to re-checkpoint dag.ckpt
        /// Create the query ckpt folder.
        UInt64 interval = process_list_element->getContext()->getSettingsRef().checkpoint_interval.value;

        /// Version + Interval + Query + Graph
        ckpt_ctx->coordinator->checkpoint(getVersion(), GRAPH_CKPT_FILE, ckpt_ctx, [&](WriteBuffer & wb) {
            /// Interval
            DB::writeIntBinary(interval, wb);
            /// Threads
            DB::writeIntBinary(execute_threads, wb);
            /// Graph
            graph->serialize(wb);
        });
    }
}

bool PipelineExecutor::hasProcessedNewDataSinceLastCheckpoint() const noexcept
{
    return graph->hasProcessedNewDataSinceLastCheckpoint();
}

void PipelineExecutor::triggerCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    graph->triggerCheckpoint(std::move(ckpt_ctx));
}

void PipelineExecutor::recover()
{
    auto & ckpt_coordinator = CheckpointCoordinator::instance(process_list_element->getContext()->getGlobalContext());

    /// Validate the execute graph doesn't change
    ckpt_coordinator.recover(process_list_element->getClientInfo().current_query_id, [&](CheckpointContextPtr ckpt_ctx) {
        recovered_epoch = ckpt_ctx->epoch;
        auto epoch_zero_ckpt_ctx = std::make_shared<CheckpointContext>(*ckpt_ctx);
        /// We will need reset epoch to zero for metadata recover since CheckpointContext::checkpointDir(...)
        /// depends on epoch
        epoch_zero_ckpt_ctx->epoch = 0;

        ckpt_ctx->coordinator->recover(GRAPH_CKPT_FILE, std::move(epoch_zero_ckpt_ctx), [&](VersionType version_, ReadBuffer & rb) {
            version = version_;

            /// Interval
            DB::readIntBinary(recovered_ckpt_interval, rb);

            /// Threads
            DB::readIntBinary(execute_threads, rb);

            /// Graph
            try
            {
                graph->deserialize(rb);
            }
            catch (const Exception & e)
            {
                LOG_ERROR(
                    log,
                    "Failed to recover query states from checkpoint : {}. Maybe the new query plan is not compatible with checkpointed. checkpointed_query={}",
                    e.message(),
                    process_list_element->getQuery());
                throw e;
            }

            /// Recover query states from checkpoint
            if (ckpt_ctx->epoch > 0)
                graph->recover(ckpt_ctx);
        });
    });
}

String PipelineExecutor::getStats() const
{
    return graph->getStats();
}

std::vector<UInt32> PipelineExecutor::getCheckpointAckNodeIDs() const
{
    std::vector<UInt32> node_ids;
    node_ids.reserve(graph->checkpoint_ack_nodes.size());
    for (const auto & node : graph->checkpoint_ack_nodes)
        node_ids.push_back(node->processor->getLogicID());

    return node_ids;
}

std::vector<UInt32> PipelineExecutor::getCheckpointSourceNodeIDs() const
{
    std::vector<UInt32> node_ids;
    node_ids.reserve(graph->checkpoint_trigger_nodes.size());
    for (const auto & node : graph->checkpoint_trigger_nodes)
        node_ids.push_back(node->processor->getLogicID());

    return node_ids;
}

VersionType PipelineExecutor::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType PipelineExecutor::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}

/// proton: ends.
}
