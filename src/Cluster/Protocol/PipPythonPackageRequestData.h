#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct PipPythonPackageRequestData final : public ProtocolData
{
    enum Operation : uint8_t
    {
        Install = 0,
        Uninstall = 1,
        /// TODO: List should be separated to a different request/response
        List = 2
    };

    /// Used for deserialization
    PipPythonPackageRequestData() = default;

    PipPythonPackageRequestData(
        Operation operation_,
        std::vector<std::string> && package_names_,
        std::vector<std::string> && package_versions_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        std::string && task_id_ = "")
        : operation(operation_)
        , package_names(std::move(package_names_))
        , package_versions(std::move(package_versions_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
        , task_id(std::move(task_id_))
    {
    }

    ~PipPythonPackageRequestData() override;

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::PipPythonPackage; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    Operation operation = Operation::Install;
    std::vector<std::string> package_names;
    std::vector<std::string> package_versions;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
    std::string task_id;
};

using PipPythonPackageRequestDataPtr = std::shared_ptr<PipPythonPackageRequestData>;
}
