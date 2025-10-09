#pragma once

#include <Cluster/Protocol/AbortMaterializedViewRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Abort a materialized view
struct AbortMaterializedViewRequest final : public Request
{
public:
    using Request::Request;

    AbortMaterializedViewRequest(
        const StreamShard & mv_ckpt_stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(mv_ckpt_stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::AbortMaterializedViewRequestData & data() override { return request_data; }

    const protocol::AbortMaterializedViewRequestData & data() const override { return request_data; }

private:
    protocol::AbortMaterializedViewRequestData request_data;
};

using AbortMaterializedViewRequestPtr = std::shared_ptr<AbortMaterializedViewRequest>;
}
