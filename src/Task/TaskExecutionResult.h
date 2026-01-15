#pragma once

#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/Error.h>
#include <Interpreters/StorageID.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

#include <unordered_map>


namespace DB::Task
{
using Checkpoint = std::unordered_map<std::string, std::string>;

cluster::CallResultV<Checkpoint> parseCheckpoint(const std::string & checkpoint_str);

struct TaskExecutionResult
{
    Checkpoint checkpoint;
    UInt64 execution_node;
    Int64 execution_start;
    Int64 execution_end;
    cluster::Error error;

    [[nodiscard]] std::string displayError() const
    {
        return error.hasError() ? fmt::format("{}: {}", ErrorCodes::getName(error.error_code), error.error_message) : "OK";
    }
};

int saveTaskExecutionResult(StorageID id, uint32_t version, const TaskExecutionResult & result);

cluster::CallResultV<TaskExecutionResult> loadTaskExecutionResult(UUID id, uint32_t version, const std::string & checkpoint_init_values);
}
