#pragma once

#include <Cluster/Protocol/GetStreamResponseData.h>
#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct GetStreamResponse final : public Response
{
public:
    using Response::Response;

    GetStreamResponse(
        protocol::StreamDescriptorPtrs && stream_descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(stream_descs), replica_leader_, sn_)
    {
    }

    GetStreamResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetStreamResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetStreamResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetStreamResponseData & data() override { return response_data; }

    const protocol::GetStreamResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetStreamResponseData response_data;
};

using GetStreamResponsePtr = std::shared_ptr<GetStreamResponse>;
}
