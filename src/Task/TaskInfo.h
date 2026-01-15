#pragma once

#include <Cluster/Protocol/TaskDescriptor.h>
#include <Interpreters/StorageID.h>

#include <optional>


namespace DB::Task
{

struct TaskInfo
{
    explicit TaskInfo(cluster::protocol::TaskDescriptorPtr descriptor_);

    cluster::protocol::TaskDescriptorPtr descriptor;
    std::vector<StorageID> source_tables;
    std::optional<StorageID> target_table;
};

using TaskInfoPtr = std::shared_ptr<TaskInfo>;

}
