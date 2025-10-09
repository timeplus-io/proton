#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/StreamShard.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
struct MaterializedViewAssignment
{
public:
    MaterializedViewAssignment(
        Stream && mv_, NodeID assign_node_, NodeID worker_node_, const NodeUUID & worker_node_uuid_, NodeID reassign_from);

    MaterializedViewAssignment() = default;

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    size_t approximateSerializedSize() const noexcept;

    std::string string() const;

public:
    constexpr static uint32_t schema_version = 1;

    uint64_t data_version = 1;

    Stream mv;
    /// Node which initiates the assignment (meta leader node)
    NodeID assign_node = Nulls::NullNodeID;
    /// If reassign_from is not null node, this request is reassignment
    NodeID reassign_from = Nulls::NullNodeID;
    /// worker_node which is assigned to execute MV
    NodeID worker_node = Nulls::NullNodeID;
    NodeUUID worker_node_uuid = Nulls::NullNodeUUID;

    int64_t timestamp = 0;
};

using MaterializedViewAssignmentPtr = std::shared_ptr<MaterializedViewAssignment>;
using MaterializedViewAssignmentPtrs = std::vector<MaterializedViewAssignmentPtr>;

}
