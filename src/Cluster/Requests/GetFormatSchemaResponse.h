#pragma once

#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/GetFormatSchemaResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct GetFormatSchemaResponse final : public Response
{
public:
    using Response::Response;

    GetFormatSchemaResponse(
        protocol::FormatSchemaDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    GetFormatSchemaResponse(
        protocol::FormatSchemaDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::FormatSchemaDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    GetFormatSchemaResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetFormatSchemaResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetFormatSchemaResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetFormatSchemaResponseData & data() override { return response_data; }

    const protocol::GetFormatSchemaResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetFormatSchemaResponseData response_data;
};

using GetFormatSchemaResponsePtr = std::shared_ptr<GetFormatSchemaResponse>;
}
