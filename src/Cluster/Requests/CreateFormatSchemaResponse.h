#pragma once

#include <Cluster/Protocol/CreateFormatSchemaResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct CreateFormatSchemaResponse final : public Response
{
public:
    using Response::Response;

    CreateFormatSchemaResponse(
        protocol::FormatSchemaDescriptor && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(desc), replica_leader_, sn_)
    {
    }

    CreateFormatSchemaResponse(
        const protocol::FormatSchemaDescriptor & desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(desc, replica_leader_, sn_)
    {
    }

    CreateFormatSchemaResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateFormatSchemaResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateFormatSchemaResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::CreateFormatSchemaResponseData & data() override { return response_data; }

    const protocol::CreateFormatSchemaResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateFormatSchemaResponseData response_data;
};

using CreateFormatSchemaResponsePtr = std::shared_ptr<CreateFormatSchemaResponse>;
}
