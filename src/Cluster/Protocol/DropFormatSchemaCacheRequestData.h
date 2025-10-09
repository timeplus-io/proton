#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <unordered_set>

namespace cluster::protocol
{
struct DropFormatSchemaCacheRequestData final : public ProtocolData
{
    /// Used for deserialization
    DropFormatSchemaCacheRequestData() = default;

    DropFormatSchemaCacheRequestData(std::unordered_set<std::string> && caches_to_drop_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : caches_to_drop(std::move(caches_to_drop_)), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DropFormatSchemaCache; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::unordered_set<std::string> caches_to_drop;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DropFormatSchemaCacheRequestDataPtr = std::shared_ptr<DropFormatSchemaCacheRequestData>;
}
