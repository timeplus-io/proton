#pragma once

#include <Cluster/Protocol/DeleteDatabaseResponseData.h>
#include <Cluster/Requests/Response.h>


namespace cluster
{

struct DeleteDatabaseResponse final : public Response
{
public:
    using Response::Response;

    DeleteDatabaseResponse(cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(leader_replica_, sn_)
    {
    }

    DeleteDatabaseResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteDatabaseResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    DeleteDatabaseResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::DeleteDatabaseResponseData & data() override { return response_data; }

    const protocol::DeleteDatabaseResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteDatabaseResponseData response_data;
};

using DeleteDatabaseResponsePtr = std::shared_ptr<DeleteDatabaseResponse>;
}
