#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Protocol/ProtocolData.h>


namespace cluster::protocol
{
struct ListNamedCollectionsResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    ListNamedCollectionsResponseData() = default;

    ListNamedCollectionsResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit ListNamedCollectionsResponseData(Error && err_) : err(std::move(err_)) { }

    explicit ListNamedCollectionsResponseData(std::vector<std::string> collection_names_, cluster::NodeID replica_leader_, int64_t sn_)
        : collection_names(std::move(collection_names_)), replica_leader(replica_leader_), sn(sn_)
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::ListNamedCollections; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

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

    std::vector<std::string> collection_names;

    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using ListNamedCollectionsResponseDataPtr = std::shared_ptr<ListNamedCollectionsResponseData>;
}
