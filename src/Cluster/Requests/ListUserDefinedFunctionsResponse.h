#pragma once

#include <Cluster/Protocol/ListUserDefinedFunctionsResponseData.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct ListUserDefinedFunctionsResponse final : public Response
{
public:
    using Response::Response;

    ListUserDefinedFunctionsResponse(
        protocol::UserDefinedFunctionDescriptorPtrs && descs_, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs_), replica_leader_, sn_)
    {
    }

    ListUserDefinedFunctionsResponse(
        protocol::UserDefinedFunctionDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::UserDefinedFunctionDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    ListUserDefinedFunctionsResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListUserDefinedFunctionsResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListUserDefinedFunctionsResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListUserDefinedFunctionsResponseData & data() override { return response_data; }

    const protocol::ListUserDefinedFunctionsResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListUserDefinedFunctionsResponseData response_data;
};

using ListUserDefinedFunctionsResponsePtr = std::shared_ptr<ListUserDefinedFunctionsResponse>;
}
