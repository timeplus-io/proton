#pragma once

#include <Cluster/Protocol/DeleteUserDefinedFunctionResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct DeleteUserDefinedFunctionResponse final : public Response
{
public:
    using Response::Response;

    DeleteUserDefinedFunctionResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteUserDefinedFunctionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteUserDefinedFunctionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteUserDefinedFunctionResponse(Error && error, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(error))
    {
    }

    protocol::DeleteUserDefinedFunctionResponseData & data() override { return response_data; }

    const protocol::DeleteUserDefinedFunctionResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteUserDefinedFunctionResponseData response_data;
};

using DeleteUserDefinedFunctionResponsePtr = std::shared_ptr<DeleteUserDefinedFunctionResponse>;
}
