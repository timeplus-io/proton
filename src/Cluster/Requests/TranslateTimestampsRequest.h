#pragma once

#include <Cluster/Protocol/TranslateTimestampsRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a stream by name or all streams if stream name is empty
struct TranslateTimestampsRequest final : public Request
{
public:
    using Request::Request;

    TranslateTimestampsRequest(
        std::string && ns_,
        std::string && stream_,
        const StreamID & stream_id_,
        std::vector<protocol::TranslateTimestampsRequestData::ShardTimestamp> && shard_timestamps_,
        bool append_time_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(std::move(ns_), std::move(stream_), stream_id_, std::move(shard_timestamps_), append_time_)
    {
    }

    protocol::TranslateTimestampsRequestData & data() override { return request_data; }

    const protocol::TranslateTimestampsRequestData & data() const override { return request_data; }

private:
    protocol::TranslateTimestampsRequestData request_data;
};

using TranslateTimestampsRequestPtr = std::shared_ptr<TranslateTimestampsRequest>;
}
