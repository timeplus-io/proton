#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct DeleteDatabaseResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteDatabaseResponseData() = default;

    DeleteDatabaseResponseData(cluster::NodeID replica_leader_, int64_t sn_) : replica_leader(replica_leader_), sn(sn_) { }

    DeleteDatabaseResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit DeleteDatabaseResponseData(Error && err_) : err(std::move(err_)) { }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteDatabase; }

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

using DeleteDatabaseResponseDataPtr = std::shared_ptr<DeleteDatabaseResponseData>;
}
