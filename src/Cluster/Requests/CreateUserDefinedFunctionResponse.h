#pragma once

#include <Cluster/Protocol/CreateUserDefinedFunctionResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct CreateUserDefinedFunctionResponse final : public Response
{
public:
    using Response::Response;

    CreateUserDefinedFunctionResponse(
        protocol::UserDefinedFunctionDescriptor && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(desc), replica_leader_, sn_)
    {
    }

    CreateUserDefinedFunctionResponse(
        const protocol::UserDefinedFunctionDescriptor & desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(desc, replica_leader_, sn_)
    {
    }

    CreateUserDefinedFunctionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateUserDefinedFunctionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateUserDefinedFunctionResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::CreateUserDefinedFunctionResponseData & data() override { return response_data; }

    const protocol::CreateUserDefinedFunctionResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateUserDefinedFunctionResponseData response_data;
};

using CreateUserDefinedFunctionResponsePtr = std::shared_ptr<CreateUserDefinedFunctionResponse>;
}
