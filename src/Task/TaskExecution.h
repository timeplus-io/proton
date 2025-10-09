#pragma once

#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>


namespace cluster::protocol
{
struct TaskDescriptor;
}

namespace DB::Task
{

class TaskScheduler;
using TaskSchedulerPtr = std::shared_ptr<TaskScheduler>;

struct TaskExecutionState;

class TaskExecution
{
public:
    TaskExecution(const UUID & task_id_, uint32_t data_version_, TaskSchedulerPtr task_scheduler_, int64_t next_run_sec_)
        : task_id(task_id_)
        , data_version(data_version_)
        , next_run_sec(next_run_sec_)
        , task_scheduler(std::move(task_scheduler_))
        , logger(getLogger("TaskExecution"))
    {
    }

    void execute();

private:
    std::optional<std::string> getQuery(const cluster::protocol::TaskDescriptor & task, TaskExecutionState & state);
    void executeQuery(
        const std::string & query,
        const ContextPtr & query_context,
        const cluster::protocol::TaskDescriptor & task,
        TaskExecutionState & state);

    UUID task_id;
    uint32_t data_version;
    int64_t next_run_sec;
    TaskSchedulerPtr task_scheduler;

    LoggerPtr logger;
};

}
