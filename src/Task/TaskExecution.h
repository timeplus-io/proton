#pragma once

#include <Cluster/Protocol/TaskDescriptor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Task/TaskExecutionResult.h>
#include <Common/Logger.h>


namespace DB::Task
{
class TaskExecution
{
public:
    explicit TaskExecution(StorageID task_id_, std::optional<uint32_t> data_version = std::nullopt)
        : task_id(std::move(task_id_)), logger(getLogger("TaskExecution"))
    {
        getAndCheckTaskDescriptor(data_version);
    }

    TaskExecutionResult execute(const ContextPtr & context);
    void cancel() noexcept;

private:
    void getAndCheckTaskDescriptor(std::optional<uint32_t> data_version);

    std::pair<std::string, Checkpoint> getQueryAndCheckpoint();
    Checkpoint executeQuery(const std::string & query, const ContextPtr & query_context);

    StorageID task_id;
    cluster::protocol::TaskDescriptorPtr task_descriptor;

    std::atomic<bool> is_canceled{false};

    LoggerPtr logger;
};
}
