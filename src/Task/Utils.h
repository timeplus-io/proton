#pragma once

#include <Interpreters/StorageID.h>
#include <Task/TaskExecutionState.h>

#include <optional>
#include <string>
#include <vector>


namespace DB::Task
{

struct TaskInfo
{
    explicit TaskInfo(StorageID id_, uint32_t status_) : id(std::move(id_)), status(status_) { }

    StorageID id;
    uint32_t status;
    
    std::vector<StorageID> source_tables;
    std::optional<StorageID> target_table;
};

std::vector<TaskInfo> getTaskInfos(const std::string & ns);

std::string getTaskCompleteQuery(const std::string & sql, const Checkpoint & checkpoint);


}
