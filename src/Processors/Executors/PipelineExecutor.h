#pragma once

#include <Processors/IProcessor.h>
#include <Processors/Executors/ExecutorTasks.h>
#include <Common/EventCounter.h>
#include <Common/logger_useful.h>

/// proton : starts
#include <Checkpoint/CheckpointContextFwd.h>
#include "Core/ExecuteMode.h"
/// proton : ends

#include <queue>
#include <stack>
#include <mutex>

namespace DB
{

class QueryStatus;
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
    explicit PipelineExecutor(Processors & processors, QueryStatus * elem);
    ~PipelineExecutor();

    /// Execute pipeline in multiple threads. Must be called once.
    /// In case of exception during execution throws any occurred.
    void execute(size_t num_threads, ExecuteMode exec_mode_ = ExecuteMode::NORMAL);

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
    void registerCheckpoint(ExecuteMode exec_mode_);
    void deregisterCheckpoint();

    bool requireExplicitCancel() const { return exec_mode == ExecuteMode::SUBSCRIBE || exec_mode == ExecuteMode::RECOVER;}

    String getStats() const;

    bool hasProcessedToCheckpoint() const;

    /// Trigger checkpointing the states of operators in the graph
    void triggerCheckpoint(CheckpointContextPtr ckpt_ctx);

    /// Recover the query graph from storage
    void recover();

    /// Persistent query graph to storage when query gets executed
    void serialize(CheckpointContextPtr ckpt_ctx) const;

    /// Return ack node IDs in the graph
    std::vector<UInt32> getCheckpointAckNodeIDs() const;
    /// Return source node IDs in the graph
    std::vector<UInt32> getCheckpointSourceNodeIDs() const;

    VersionType getVersionFromRevision(UInt64 revision) const;
    VersionType getVersion() const;

    ExecutingGraph & getExecGraph() const { return *graph; }

    bool isCancelled() const { return cancelled; }
    /// proton: ends.

private:
    ExecutingGraphPtr graph;

    ExecutorTasks tasks;
    using Stack = std::stack<UInt64>;

    /// Flag that checks that initializeExecution was called.
    bool is_execution_initialized = false;
    /// system.processors_profile_log
    bool profile_processors = false;

    std::atomic_bool cancelled = false;

    /// proton : starts
    std::mutex register_checkpoint_mutex;
    ExecuteMode exec_mode = ExecuteMode::NORMAL;
    UInt16 execute_threads = 0;
    mutable std::optional<VersionType> version;
    UInt64 recovered_ckpt_interval = 0;
    Int64 recovered_epoch = 0;
    /// proton : ends

    Poco::Logger * log = &Poco::Logger::get("PipelineExecutor");

    /// Now it's used to check if query was killed.
    QueryStatus * const process_list_element = nullptr;

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
