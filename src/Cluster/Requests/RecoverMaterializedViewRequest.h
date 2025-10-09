#pragma once

#include <Cluster/Protocol/RecoverMaterializedViewRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Recover a materialized view
struct RecoverMaterializedViewRequest final : public Request
{
public:
    using Request::Request;

    RecoverMaterializedViewRequest(const StreamShard & mv_ckpt_stream_shard_,  cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(mv_ckpt_stream_shard_, initiator_, timeout_ms_)
    {
    }

    protocol::RecoverMaterializedViewRequestData & data() override { return request_data; }

    const protocol::RecoverMaterializedViewRequestData & data() const override { return request_data; }

private:
    protocol::RecoverMaterializedViewRequestData request_data;
};

using RecoverMaterializedViewRequestPtr = std::shared_ptr<RecoverMaterializedViewRequest>;
}
