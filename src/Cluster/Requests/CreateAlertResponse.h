#pragma once

#include <Cluster/Protocol/CreateAlertResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct CreateAlertResponse final : public Response
{
public:
    using Response::Response;

    CreateAlertResponse(
        protocol::AlertDescriptor && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(desc), replica_leader_, sn_)
    {
    }

    CreateAlertResponse(
        const protocol::AlertDescriptor & desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(desc, replica_leader_, sn_)
    {
    }

    CreateAlertResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateAlertResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateAlertResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::CreateAlertResponseData & data() override { return response_data; }

    const protocol::CreateAlertResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateAlertResponseData response_data;
};

using CreateAlertResponsePtr = std::shared_ptr<CreateAlertResponse>;
}
