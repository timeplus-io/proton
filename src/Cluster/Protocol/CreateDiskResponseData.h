#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct CreateDiskResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    CreateDiskResponseData() = default;

    CreateDiskResponseData(cluster::NodeID replica_leader_, int64_t sn_) : replica_leader(replica_leader_), sn(sn_) { }

    CreateDiskResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit CreateDiskResponseData(Error && err_) : err(std::move(err_)) { }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateDisk; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    std::string doString() const override;

public:
    Error err;

    /// Added in v2 schema
    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using CreateDiskResponseDataPtr = std::shared_ptr<CreateDiskResponseData>;
}
