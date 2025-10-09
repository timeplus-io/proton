#pragma once

#include <Cluster/Protocol/PauseStreamShardRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Pause a stream shard
struct PauseStreamShardRequest final : public Request
{
public:
    using Request::Request;

    PauseStreamShardRequest(const StreamIDShard & stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::PauseStreamShardRequestData & data() override { return request_data; }

    const protocol::PauseStreamShardRequestData & data() const override { return request_data; }

private:
    protocol::PauseStreamShardRequestData request_data;
};

using PauseStreamShardRequestPtr = std::shared_ptr<PauseStreamShardRequest>;
}
