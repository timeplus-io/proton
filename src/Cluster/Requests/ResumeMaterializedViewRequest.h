#pragma once

#include <Cluster/Protocol/ResumeMaterializedViewRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Resume a materialized view
struct ResumeMaterializedViewRequest final : public Request
{
public:
    using Request::Request;

    ResumeMaterializedViewRequest(
        const StreamShard & mv_ckpt_stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(mv_ckpt_stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::ResumeMaterializedViewRequestData & data() override { return request_data; }

    const protocol::ResumeMaterializedViewRequestData & data() const override { return request_data; }

private:
    protocol::ResumeMaterializedViewRequestData request_data;
};

using ResumeMaterializedViewRequestPtr = std::shared_ptr<ResumeMaterializedViewRequest>;
}
