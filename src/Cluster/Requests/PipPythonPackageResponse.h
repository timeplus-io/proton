#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/PipPythonPackageResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
/// SYSTEM PYTHON PACKAGE operation response
struct PipPythonPackageResponse final : public Response
{
public:
    using Response::Response;

    explicit PipPythonPackageResponse(Error && err_, uint16_t response_version_)
        : Response(response_version_), response_data(std::move(err_))
    {
    }

    PipPythonPackageResponse(int32_t error_code, std::string_view error_message, uint16_t response_version_)
        : Response(response_version_), response_data(error_code, std::string(error_message))
    {
    }

    PipPythonPackageResponse(int32_t error_code, std::string && error_message, uint16_t response_version_)
        : Response(response_version_), response_data(error_code, std::move(error_message))
    {
    }

    PipPythonPackageResponse(int64_t sn_, uint16_t response_version_) : Response(response_version_), response_data(sn_) { }

    PipPythonPackageResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t response_version_)
        : Response(response_version_), response_data(replica_leader_, sn_)
    {
    }

    /// Constructor for LIST operation response with package data
    PipPythonPackageResponse(
        std::vector<std::string> && package_names_,
        std::vector<std::string> && package_versions_,
        cluster::NodeID replica_leader_,
        int64_t sn_,
        uint16_t response_version_)
        : Response(response_version_), response_data(std::move(package_names_), std::move(package_versions_), replica_leader_, sn_)
    {
    }

    protocol::PipPythonPackageResponseData & data() override { return response_data; }

    const protocol::PipPythonPackageResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }
    std::string errorString() const { return response_data.errorString(); }

private:
    protocol::PipPythonPackageResponseData response_data;
};

using PipPythonPackageResponsePtr = std::shared_ptr<PipPythonPackageResponse>;
}
