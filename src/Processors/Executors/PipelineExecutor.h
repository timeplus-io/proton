#pragma once

#include <Processors/Executors/ExecutorTasks.h>
#include <Processors/IProcessor.h>
#include <Common/EventCounter.h>

/// proton : starts
#include <Checkpoint/CheckpointContextFwd.h>
#include <Core/ExecuteMode.h>
/// proton : ends

#include <mutex>
#include <queue>
#include <stack>

namespace DB
{
/// proton: starts.
struct CheckpointSettings;
using CheckpointSettingsPtr = std::shared_ptr<CheckpointSettings>;
/// proton: ends.

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ExecutingGraph;
using ExecutingGraphPtr = std::unique_ptr<ExecutingGraph>;

class ReadProgressCallback;
using ReadProgressCallbackPtr = std::unique_ptr<ReadProgressCallback>;


/// Executes query pipeline.
class PipelineExecutor final : public std::enable_shared_from_this<PipelineExecutor>
{
public:
    /// Get pipeline as a set of processors.
    /// Processors should represent full graph. All ports must be connected, all connected nodes are mentioned in set.
    /// Executor doesn't own processors, just stores reference.
    /// During pipeline execution new processors can appear. They will be added to existing set.
    ///
    /// Explicit graph representation is built in constructor. Throws if graph is not correct.
    explicit PipelineExecutor(std::shared_ptr<Processors> & processors, QueryStatusPtr elem);
    ~PipelineExecutor();

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads, ExecuteMode exec_mode_ = ExecuteMode::Normal);

    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.
    bool executeStep(std::atomic_bool * yield_flag = nullptr);

    const Processors & getProcessors() const;

    /// Cancel execution. May be called from another thread.
    void cancel();

    /// Checks the query time limits (cancelled or timeout). Throws on cancellation or when time limit is reached and the query uses "break"
    bool checkTimeLimit();
    /// Same as checkTimeLimit but it never throws. It returns false on cancellation or time limit reached
    [[nodiscard]] bool checkTimeLimitSoft();

    /// Set callback for read progress.
    /// It would be called every time when processor reports read progress.
    void setReadProgressCallback(ReadProgressCallbackPtr callback);

    /// proton: starts.
    void registerCheckpoint(ExecuteMode exec_mode_, CheckpointContextPtr ckpt_ctx = {});
    void deregisterCheckpoint();

    bool checkpointRegistered() const;
    bool requireExplicitCancel() const { return checkpointRegistered(); }

    String getStats() const;

    bool hasProcessedNewDataSinceLastCheckpoint() const noexcept;

    /// Trigger checkpointing the states of operators in the graph
    void triggerCheckpoint(CheckpointContextPtr ckpt_ctx);

    /// Persistent query graph when query gets executed
    void checkpointGraph(WriteBuffer & wb) const;

    VersionType getVersionFromRevision(UInt64 revision) const;
    VersionType getVersion() const;

    ExecutingGraph & getExecGraph() const { return *graph; }

    bool isCancelled() const { return cancelled; }

private:
    /// Recover the query graph from storage
    std::pair<Int64, CheckpointSettingsPtr> recover(CheckpointContextPtr ckpt_ctx);
    /// proton: ends.

private:
    ExecutingGraphPtr graph;

    ExecutorTasks tasks;
    using Stack = std::stack<UInt64>;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    /// system.processors_profile_log
    bool profile_processors = false;
    /// system.opentelemetry_span_log
    bool trace_processors = false;

    std::atomic_bool cancelled = false;

    /// proton : starts
    mutable std::mutex register_checkpoint_mutex;
    ExecuteMode exec_mode = ExecuteMode::Normal;
    UInt16 execute_threads = 0;
    mutable std::optional<VersionType> version;
    /// proton : ends

    LoggerPtr log = getLogger("PipelineExecutor");

    /// Now it's used to check if query was killed.
    QueryStatusPtr process_list_element;

    ReadProgressCallbackPtr read_progress_callback;

    using Queue = std::queue<ExecutingGraph::Node *>;

    void initializeExecution(size_t num_threads); /// Initialize executor contexts and task_queue.
    void finalizeExecution(); /// Check all processors are finished.

    /// Methods connected to execution.
    void executeImpl(size_t num_threads, ExecuteMode exec_mode_);
    void executeStepImpl(size_t thread_num, std::atomic_bool * yield_flag = nullptr);
    void executeSingleThread(size_t thread_num);
    void finish();

    String dumpPipeline() const;
};

using PipelineExecutorPtr = std::shared_ptr<PipelineExecutor>;

}
