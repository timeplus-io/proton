#pragma once

#include <Cluster/Protocol/ResumeStreamShardRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Resume a stream shard
struct ResumeStreamShardRequest final : public Request
{
public:
    using Request::Request;

    ResumeStreamShardRequest(
        const StreamIDShard & stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::ResumeStreamShardRequestData & data() override { return request_data; }

    const protocol::ResumeStreamShardRequestData & data() const override { return request_data; }

private:
    protocol::ResumeStreamShardRequestData request_data;
};

using ResumeStreamShardRequestPtr = std::shared_ptr<ResumeStreamShardRequest>;
}
