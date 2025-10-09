#pragma once

#include <Cluster/Protocol/DeleteAccessEntityResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct DeleteAccessEntityResponse final : public Response
{
public:
    using Response::Response;

    DeleteAccessEntityResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteAccessEntityResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    DeleteAccessEntityResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteAccessEntityResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::DeleteAccessEntityResponseData & data() override { return response_data; }

    const protocol::DeleteAccessEntityResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteAccessEntityResponseData response_data;
};

using DeleteAccessEntityResponsePtr = std::shared_ptr<DeleteAccessEntityResponse>;
}
