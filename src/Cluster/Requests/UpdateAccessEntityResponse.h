#pragma once

#include <Cluster/Protocol/UpdateAccessEntityResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct UpdateAccessEntityResponse final : public Response
{
public:
    using Response::Response;

    UpdateAccessEntityResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    UpdateAccessEntityResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    UpdateAccessEntityResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    UpdateAccessEntityResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::UpdateAccessEntityResponseData & data() override { return response_data; }

    const protocol::UpdateAccessEntityResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::UpdateAccessEntityResponseData response_data;
};

using UpdateAccessEntityResponsePtr = std::shared_ptr<UpdateAccessEntityResponse>;
}
