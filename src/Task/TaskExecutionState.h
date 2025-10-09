#pragma once

#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/Error.h>
#include <Core/UUID.h>


namespace DB::Task
{
using Checkpoint = std::unordered_map<std::string, std::string>;

cluster::CallResultV<Checkpoint> parseCheckpoint(std::string checkpoint_str);

struct TaskExecutionState
{
    Checkpoint checkpoint;
    Int64 execution_start;
    Int64 execution_end;
    cluster::Error error;
};

int saveTaskExecutionState(UUID id, uint32_t data_version, const TaskExecutionState & state);

cluster::CallResultV<TaskExecutionState> loadTaskExecutionState(UUID id, uint32_t data_version, const std::string & checkpoint_init_values);
}
