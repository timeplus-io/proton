#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>


namespace cluster::protocol
{
struct GetNamedCollectionResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    GetNamedCollectionResponseData() = default;

    GetNamedCollectionResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit GetNamedCollectionResponseData(Error && err_) : err(std::move(err_)) { }

    explicit GetNamedCollectionResponseData(
        NamedCollectionDescriptorPtrs && named_collections_, cluster::NodeID replica_leader_, int64_t sn_)
        : named_collections(std::move(named_collections_)), replica_leader(replica_leader_), sn(sn_)
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::GetNamedCollection; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    /// Marshal the request / response to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override;

    [[nodiscard]] bool hasError() const noexcept { return err.hasError(); }
    [[nodiscard]] const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    [[nodiscard]] std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;
    [[nodiscard]] std::string doString() const override;

public:
    Error err;
    NamedCollectionDescriptorPtrs named_collections;

    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using GetNamedCollectionResponseDataPtr = std::shared_ptr<GetNamedCollectionResponseData>;
}
