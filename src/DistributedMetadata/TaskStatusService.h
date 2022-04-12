#pragma once

#include "MetadataService.h"

#include <Core/BackgroundSchedulePool.h>

#include <optional>

namespace DB
{
class TaskStatusService final : public MetadataService
{
public:
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
    static TaskStatusService & instance(const ContextMutablePtr & global_context_);

    explicit TaskStatusService(const ContextMutablePtr & global_context_);
    ~TaskStatusService() override = default;

    Int32 append(TaskStatusPtr task);

    TaskStatusPtr findById(const String & id);
    std::vector<TaskStatusPtr> findByUser(const String & user);

    void createTaskTableIfNotExists();
    bool ready() const override { return started.test() && table_exists; }

private:
    void processRecords(const nlog::RecordPtrs & records) override;

    String role() const override { return "task"; }
    ConfigSettings configSettings() const override;

    TaskStatusPtr findByIdInMemory(const String & id);
    TaskStatusPtr findByIdInTable(const String & id);

    void findByUserInMemory(const String & user, std::vector<TaskStatusPtr> & res);
    void findByUserInTable(const String & user, std::vector<TaskStatusPtr> & res);

    TaskStatusPtr buildTaskStatusFromRecord(const nlog::RecordPtr & record) const;

    void buildTaskStatusFromBlock(const Block & block, std::vector<TaskStatusPtr> & res) const;

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;
    void updateTaskStatus(TaskStatusPtr & task);

    bool tableExists();
    bool createTaskTable();

    bool persistentTaskStatuses(std::vector<TaskStatusPtr> tasks);
    void cleanupCachedTask();
    void doCleanupCachedTask();
    void cleanupTaskByIdUnlocked(const String & id);

private:
    mutable std::shared_mutex indexes_lock;
    std::unordered_map<String, TaskStatusPtr> indexed_by_id;
    std::unordered_map<String, std::unordered_map<String, TaskStatusPtr>> indexed_by_user;
    std::vector<TaskStatusPtr> finished_tasks;

    UInt64 max_cached_size_threshold = 0;

    std::atomic_bool table_exists = false;

    static constexpr Int32 RETRY_TIMES = 3;
    static constexpr size_t RETRY_INTERVAL_MS = 5000;
};
}
