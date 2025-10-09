#pragma once

#include <Cluster/Protocol/DeleteDiskResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct DeleteDiskResponse final : public Response
{
public:
    using Response::Response;

    DeleteDiskResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteDiskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteDiskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    DeleteDiskResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::DeleteDiskResponseData & data() override { return response_data; }

    const protocol::DeleteDiskResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteDiskResponseData response_data;
};

using DeleteDiskResponsePtr = std::shared_ptr<DeleteDiskResponse>;
}
