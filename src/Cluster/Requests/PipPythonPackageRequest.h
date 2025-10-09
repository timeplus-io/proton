#pragma once

#include <Cluster/Protocol/PipPythonPackageRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// SYSTEM PYTHON PACKAGE operation
struct PipPythonPackageRequest final : public Request
{
public:
    using Request::Request;

    PipPythonPackageRequest(
        protocol::PipPythonPackageRequestData::Operation operation_,
        std::vector<std::string> && package_names_,
        std::vector<std::string> && package_versions_,
        NodeID initiator_,
        int64_t timeout_ms,
        uint16_t request_version_,
        std::string && task_id_ = "")
        : Request(request_version_)
        , request_data(operation_, std::move(package_names_), std::move(package_versions_), initiator_, timeout_ms, std::move(task_id_))
    {
    }

    protocol::PipPythonPackageRequestData & data() override { return request_data; }

    const protocol::PipPythonPackageRequestData & data() const override { return request_data; }

private:
    protocol::PipPythonPackageRequestData request_data;
};

using PipPythonPackageRequestPtr = std::shared_ptr<PipPythonPackageRequest>;
}
