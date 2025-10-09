#pragma once

#include <Cluster/Protocol/DeleteFormatSchemaResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct DeleteFormatSchemaResponse final : public Response
{
public:
    using Response::Response;

    DeleteFormatSchemaResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteFormatSchemaResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteFormatSchemaResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteFormatSchemaResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error))
    {
    }

    protocol::DeleteFormatSchemaResponseData & data() override { return response_data; }

    const protocol::DeleteFormatSchemaResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteFormatSchemaResponseData response_data;
};

using DeleteFormatSchemaResponsePtr = std::shared_ptr<DeleteFormatSchemaResponse>;
}
