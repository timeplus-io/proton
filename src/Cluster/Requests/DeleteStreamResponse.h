#pragma once

#include <Cluster/Protocol/DeleteStreamResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct DeleteStreamResponse final : public Response
{
public:
    using Response::Response;

    DeleteStreamResponse(cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(leader_replica_, sn_)
    {
    }

    DeleteStreamResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteStreamResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteStreamResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::DeleteStreamResponseData & data() override { return response_data; }

    const protocol::DeleteStreamResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteStreamResponseData response_data;
};

using DeleteStreamResponsePtr = std::shared_ptr<DeleteStreamResponse>;
}
