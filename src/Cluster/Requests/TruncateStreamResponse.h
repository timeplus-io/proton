#pragma once

#include <Cluster/Requests/Response.h>
#include <Cluster/Protocol/TruncateStreamResponseData.h>

namespace cluster
{
struct TruncateStreamResponse final : public Response
{
public:
    using Response::Response;

    TruncateStreamResponse(int32_t error_code, std::string && error_message, uint16_t data_version_) : Response(data_version_), response_data(error_code, std::move(error_message)) { }

    protocol::TruncateStreamResponseData & data() override { return response_data; }

    const protocol::TruncateStreamResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::TruncateStreamResponseData response_data;
};

using TruncateStreamResponsePtr = std::shared_ptr<TruncateStreamResponse>;
}
