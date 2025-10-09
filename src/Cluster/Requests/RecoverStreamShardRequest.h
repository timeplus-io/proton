#pragma once

#include <Cluster/Protocol/RecoverStreamShardRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Recover a stream shard
struct RecoverStreamShardRequest final : public Request
{
public:
    using Request::Request;

    RecoverStreamShardRequest(
        const StreamIDShard & stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::RecoverStreamShardRequestData & data() override { return request_data; }

    const protocol::RecoverStreamShardRequestData & data() const override { return request_data; }

private:
    protocol::RecoverStreamShardRequestData request_data;
};

using RecoverStreamShardRequestPtr = std::shared_ptr<RecoverStreamShardRequest>;
}
