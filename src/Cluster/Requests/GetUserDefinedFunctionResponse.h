#pragma once

#include <Cluster/Protocol/GetUserDefinedFunctionResponseData.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct GetUserDefinedFunctionResponse final : public Response
{
public:
    using Response::Response;

    GetUserDefinedFunctionResponse(
        protocol::UserDefinedFunctionDescriptorPtrs && descs_, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs_), replica_leader_, sn_)
    {
    }

    GetUserDefinedFunctionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetUserDefinedFunctionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetUserDefinedFunctionResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetUserDefinedFunctionResponseData & data() override { return response_data; }

    const protocol::GetUserDefinedFunctionResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetUserDefinedFunctionResponseData response_data;
};

using GetUserDefinedFunctionResponsePtr = std::shared_ptr<GetUserDefinedFunctionResponse>;
}
