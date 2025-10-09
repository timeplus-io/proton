#pragma once

#include <Cluster/Protocol/DatabaseDescriptor.h>
#include <Cluster/Protocol/GetDatabaseResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct GetDatabaseResponse final : public Response
{
public:
    using Response::Response;

    GetDatabaseResponse(protocol::DatabaseDescriptorPtrs && databases, cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(databases), leader_replica_, sn_)
    {
    }

    GetDatabaseResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetDatabaseResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetDatabaseResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetDatabaseResponseData & data() override { return response_data; }

    const protocol::GetDatabaseResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetDatabaseResponseData response_data;
};

using GetDatabaseResponsePtr = std::shared_ptr<GetDatabaseResponse>;
}
