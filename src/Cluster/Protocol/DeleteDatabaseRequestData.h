#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteDatabaseRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteDatabaseRequestData() = default;

    DeleteDatabaseRequestData(std::string name_, std::string ddl_query_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : name(std::move(name_)), ddl_query(std::move(ddl_query_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteDatabase; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    std::string name;
    std::string ddl_query;

    /// Added by v2 schema
    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DeleteDatabaseRequestDataPtr = std::shared_ptr<DeleteDatabaseRequestData>;
}
