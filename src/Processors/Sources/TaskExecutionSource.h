#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Processors/ISource.h>
#include <Task/TaskExecution.h>
#include <Task/TaskExecutionResult.h>


namespace DB
{
class TaskExecutionSource final : public ISource
{
public:
    TaskExecutionSource(StorageID task_id, ContextPtr context_);
    String getName() const override { return "TaskExecutionSource"; }

protected:
    Chunk generate() override;
    void onCancel() noexcept override { task_execution.cancel(); }

private:
    static Block getHeader();

    Task::TaskExecution task_execution;
    ContextPtr context;

    bool executed{false};
};
}
