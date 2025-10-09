#pragma once

#include <Cluster/Protocol/CreateDiskResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct CreateDiskResponse final : public Response
{
public:
    using Response::Response;

    CreateDiskResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t request_version_)
        : Response(request_version_), response_data(replica_leader_, sn_)
    {
    }

    CreateDiskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateDiskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateDiskResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::CreateDiskResponseData & data() override { return response_data; }

    const protocol::CreateDiskResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateDiskResponseData response_data;
};

using CreateDiskResponsePtr = std::shared_ptr<CreateDiskResponse>;
}
