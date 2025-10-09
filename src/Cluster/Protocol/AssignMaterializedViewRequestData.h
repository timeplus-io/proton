#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/StreamShard.h>
#include <Cluster/Protocol/MaterializedViewAssignment.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct AssignMaterializedViewRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    AssignMaterializedViewRequestData() = default;

    AssignMaterializedViewRequestData(
        Stream && mv_, NodeID assign_node_, NodeID worker_node_, const NodeUUID & worker_node_uuid_, NodeID reassign_from_)
        : assignment(std::move(mv_), assign_node_, worker_node_, worker_node_uuid_, reassign_from_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::AssignMaterializedView; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override { assignment.serialize(wb, version); }

    size_t approximateSerializedSize() const noexcept override { return assignment.approximateSerializedSize(); }

    std::string doString() const override { return assignment.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override { assignment.deserialize(rb, version); }

public:
    MaterializedViewAssignment assignment;
};

using AssignMaterializedViewRequestDataPtr = std::shared_ptr<AssignMaterializedViewRequestData>;
}
