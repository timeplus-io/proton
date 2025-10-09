#pragma once

#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/ListFormatSchemasResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct ListFormatSchemasResponse final : public Response
{
public:
    using Response::Response;

    ListFormatSchemasResponse(
        protocol::FormatSchemaDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    ListFormatSchemasResponse(
        protocol::FormatSchemaDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::FormatSchemaDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    ListFormatSchemasResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListFormatSchemasResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListFormatSchemasResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListFormatSchemasResponseData & data() override { return response_data; }

    const protocol::ListFormatSchemasResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListFormatSchemasResponseData response_data;
};

using ListFormatSchemasResponsePtr = std::shared_ptr<ListFormatSchemasResponse>;
}
