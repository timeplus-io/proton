#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>


namespace cluster::protocol
{
struct PipPythonPackageResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    PipPythonPackageResponseData() = default;

    ~PipPythonPackageResponseData() override = default;

    explicit PipPythonPackageResponseData(Error && err_) : err(std::move(err_)) { }

    PipPythonPackageResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    explicit PipPythonPackageResponseData(int64_t sn_) : sn(sn_) { }

    PipPythonPackageResponseData(cluster::NodeID replica_leader_, int64_t sn_) : replica_leader(replica_leader_), sn(sn_) { }

    /// Constructor for LIST operation response with package data
    PipPythonPackageResponseData(
        std::vector<std::string> && package_names_,
        std::vector<std::string> && package_versions_,
        cluster::NodeID replica_leader_,
        int64_t sn_)
        : package_names(std::move(package_names_)), package_versions(std::move(package_versions_)), replica_leader(replica_leader_), sn(sn_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::PipPythonPackage; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & /*wb*/, uint16_t /*version*/) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & /*rb*/, uint16_t /*version*/) override;

public:
    Error err;

    /// For LIST operation response
    std::vector<std::string> package_names;
    std::vector<std::string> package_versions;

    cluster::NodeID replica_leader = Nulls::NullNodeID;
    int64_t sn = 0;
};

using PipPythonPackageResponseDataPtr = std::shared_ptr<PipPythonPackageResponseData>;
}
