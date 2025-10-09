#pragma once

#include <Cluster/Common/TimeWheel/TimerService.h>
#include <Core/UUID.h>
#include <Task/TaskExecutionState.h>
#include <Common/Logger.h>

#include <boost/core/noncopyable.hpp>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace cluster
{
namespace protocol
{
struct TaskDescriptor;
using TaskDescriptorPtr = std::shared_ptr<TaskDescriptor>;
using TaskDescriptorPtrs = std::vector<TaskDescriptorPtr>;
}

class TimerTaskEntry;
using TimerTaskEntryPtr = std::shared_ptr<TimerTaskEntry>;
}

namespace DB::Task
{

class TaskScheduler final : public std::enable_shared_from_this<TaskScheduler>, private boost::noncopyable
{
public:
    TaskScheduler(size_t worker_threads, size_t max_scheduled_tasks_);
    ~TaskScheduler();

    void startup();
    void shutdown();

    void onTaskCreated(const cluster::protocol::TaskDescriptor & task);
    void onTaskDeleted(const UUID & task_id);

    cluster::protocol::TaskDescriptorPtr getTaskDescriptor(const UUID & task_id);

    void onTaskExecutionComplete(const cluster::protocol::TaskDescriptorPtr & task, std::optional<TaskExecutionState> state);

    bool isExecutor();

    cluster::protocol::TaskDescriptorPtrs getTasks(const std::string & ns) const;

private:
    void loadTaskDescriptors();
    void addOrUpdateTaskUnlocked(cluster::protocol::TaskDescriptorPtr task);
    void scheduleTask(const cluster::protocol::TaskDescriptor & task);

    mutable std::mutex task_descriptors_mutex;
    std::unordered_map<UUID, cluster::protocol::TaskDescriptorPtr> task_descriptors;
    std::unordered_set<UUID> deleted_tasks;

    size_t max_scheduled_tasks;
    cluster::TimerService timer;

    std::atomic<size_t> scheduled_tasks{0};

    std::atomic_flag started;
    std::atomic_flag stopped;

    LoggerPtr logger;
};

using TaskSchedulerPtr = std::shared_ptr<TaskScheduler>;
}
