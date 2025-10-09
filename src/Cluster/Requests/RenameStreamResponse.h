#pragma once

#include <Cluster/Protocol/RenameStreamResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct RenameStreamResponse final : public Response
{
public:
    using Response::Response;

    RenameStreamResponse(cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(leader_replica_, sn_)
    {
    }

    RenameStreamResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    RenameStreamResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    RenameStreamResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::RenameStreamResponseData & data() override { return response_data; }

    const protocol::RenameStreamResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::RenameStreamResponseData response_data;
};

using RenameStreamResponsePtr = std::shared_ptr<RenameStreamResponse>;
}
