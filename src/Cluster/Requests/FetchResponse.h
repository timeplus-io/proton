#pragma once

#include <Cluster/Protocol/FetchResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct FetchResponse;
using FetchResponsePtr = std::shared_ptr<FetchResponse>;

struct FetchResponse final : public Response
{
public:
    using Response::Response;

    FetchResponse(
        int32_t throttle_time_ms_,
        uint64_t session_id_,
        uint64_t session_epoch_,
        std::vector<protocol::FetchResponseData::FetchedStreamShardData> && fetched_streams_data_,
        uint16_t data_version_)
        : Response(data_version_), response_data(throttle_time_ms_, session_id_, session_epoch_, std::move(fetched_streams_data_))
    {
    }

    FetchResponse(
        int32_t throttle_time_ms_,
        uint64_t session_id_,
        uint64_t session_epoch_,
        int32_t error_code,
        std::string && error_message,
        uint16_t data_version_)
        : Response(data_version_), response_data(throttle_time_ms_, session_id_, session_epoch_, error_code, std::move(error_message))
    {
    }

    protocol::FetchResponseData & data() override { return response_data; }

    const protocol::FetchResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::FetchResponseData response_data;
};
}
