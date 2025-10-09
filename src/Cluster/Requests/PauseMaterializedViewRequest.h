#pragma once

#include <Cluster/Protocol/PauseMaterializedViewRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Pause a materialized view
struct PauseMaterializedViewRequest final : public Request
{
public:
    using Request::Request;

    PauseMaterializedViewRequest(
        const StreamShard & mv_ckpt_stream_shard_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(mv_ckpt_stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::PauseMaterializedViewRequestData & data() override { return request_data; }

    const protocol::PauseMaterializedViewRequestData & data() const override { return request_data; }

private:
    protocol::PauseMaterializedViewRequestData request_data;
};

using PauseMaterializedViewRequestPtr = std::shared_ptr<PauseMaterializedViewRequest>;
}
