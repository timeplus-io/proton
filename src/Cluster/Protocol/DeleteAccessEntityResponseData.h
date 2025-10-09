#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteAccessEntityResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteAccessEntityResponseData() = default;

    DeleteAccessEntityResponseData(cluster::NodeID replica_leader_, int64_t sn_) : replica_leader(replica_leader_), sn(sn_) { }

    explicit DeleteAccessEntityResponseData(Error err_) : err(std::move(err_)) { }
    DeleteAccessEntityResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteAccessEntity; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    /// Marshal the request / response to write buffer
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

    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using DeleteAccessEntityResponseDataPtr = std::shared_ptr<DeleteAccessEntityResponseData>;
}
