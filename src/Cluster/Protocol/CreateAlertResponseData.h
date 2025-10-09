#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct CreateAlertResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    CreateAlertResponseData() = default;

    CreateAlertResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit CreateAlertResponseData(Error && err_) : err(std::move(err_)) { }

    explicit CreateAlertResponseData(AlertDescriptor && desc_, cluster::NodeID replica_leader_, int64_t sn_)
        : desc(std::move(desc_)), replica_leader(replica_leader_), sn(sn_)
    {
    }

    explicit CreateAlertResponseData(const AlertDescriptor & desc_, cluster::NodeID replica_leader_, int64_t sn_)
        : desc(desc_), replica_leader(replica_leader_), sn(sn_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateAlert; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    /// Marshal the request / response to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    Error err;
    AlertDescriptor desc;

    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using CreateAlertResponseDataPtr = std::shared_ptr<CreateAlertResponseData>;
}
