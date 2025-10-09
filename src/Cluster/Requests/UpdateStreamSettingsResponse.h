#pragma once

#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/UpdateStreamSettingsResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct UpdateStreamSettingsResponse final : public Response
{
public:
    using Response::Response;

    UpdateStreamSettingsResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    UpdateStreamSettingsResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    UpdateStreamSettingsResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    UpdateStreamSettingsResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::UpdateStreamSettingsResponseData & data() override { return response_data; }

    const protocol::UpdateStreamSettingsResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::UpdateStreamSettingsResponseData response_data;
};

using UpdateStreamSettingsResponsePtr = std::shared_ptr<UpdateStreamSettingsResponse>;
}
