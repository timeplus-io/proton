#pragma once

#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/TranslateTimestampsResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct TranslateTimestampsResponse final : public Response
{
public:
    using Response::Response;

    TranslateTimestampsResponse(
        Stream && stream_,
        std::vector<protocol::TranslateTimestampsResponseData::ShardSequence> && shard_sequences_,
        uint16_t data_version_)
        : Response(data_version_), response_data(std::move(stream_), std::move(shard_sequences_))
    {
    }

    TranslateTimestampsResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    protocol::TranslateTimestampsResponseData & data() override { return response_data; }

    const protocol::TranslateTimestampsResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    std::string errorString() const override { return response_data.errorString(); }

private:
    protocol::TranslateTimestampsResponseData response_data;
};

using TranslateTimestampsResponsePtr = std::shared_ptr<TranslateTimestampsResponse>;
}
