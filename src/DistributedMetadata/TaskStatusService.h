#pragma once

#include "MetadataService.h"

#include <Core/BackgroundSchedulePool.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <optional>

namespace DB
{
class TaskStatusService final : public MetadataService
{
public:
    using SteadyClock = std::chrono::time_point<std::chrono::steady_clock>;

    struct TaskStatus
    {
        String id;
        /// Overall general status
        String status;
        /// Application specific progress status
        String progress;
        String reason;
        String user;
        String context;
        Int64 last_modified = -1;
        Int64 created = -1;

        /// Overall general status codes
        static const String SUBMITTED;
        static const String INPROGRESS;
        static const String SUCCEEDED;
        static const String FAILED;
    };

    using TaskStatusPtr = std::shared_ptr<TaskStatus>;

public:
    static TaskStatusService & instance(const ContextPtr & global_context_);

    explicit TaskStatusService(const ContextPtr & global_context_);
    virtual ~TaskStatusService() override = default;

    Int32 append(TaskStatusPtr task);

    TaskStatusPtr findById(const String & id);
    std::vector<TaskStatusPtr> findByUser(const String & user);

    void schedulePersistentTask();

private:
    void processRecords(const DWAL::RecordPtrs & records) override;

    void preShutdown() override;

    String role() const override { return "task"; }
    ConfigSettings configSettings() const override;

    TaskStatusPtr findByIdInMemory(const String & id);
    TaskStatusPtr findByIdInTable(const String & id);

    void findByUserInMemory(const String & user, std::vector<TaskStatusPtr> & res);
    void findByUserInTable(const String & user, std::vector<TaskStatusPtr> & res);

    TaskStatusPtr buildTaskStatusFromRecord(const DWAL::RecordPtr & record) const;

    void buildTaskStatusFromBlock(const Block & block, std::vector<TaskStatusPtr> & res) const;

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;
    void updateTaskStatus(TaskStatusPtr & task);

    bool tableExists();
    bool createTaskTable();

    bool persistentTaskStatuses(const std::vector<TaskStatusPtr> & tasks);
    void persistentFinishedTask();

private:
    mutable std::shared_mutex indexes_lock;
    std::unordered_map<String, TaskStatusPtr> indexed_by_id;
    std::unordered_map<String, std::unordered_map<String, TaskStatusPtr>> indexed_by_user;

    std::mutex tasks_lock;
    std::deque<std::pair<SteadyClock, String>> timed_tasks;
    std::unordered_map<Int64, std::vector<TaskStatusPtr>> finished_tasks;
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> persistent_task;

    bool table_exists = false;

    static constexpr size_t RESCHEDULE_TIME_MS = 120000;
    static constexpr Int32 RETRY_TIMES = 3;
    static constexpr size_t RETRY_INTERVAL_MS = 5000;
    static constexpr Int64 CACHE_FINISHED_TASK_MS = 30000;
};
}
