#pragma once

#include <Cluster/Protocol/AssignMaterializedViewRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Assign a materialized view to a worker node to execute
struct AssignMaterializedViewRequest final : public Request
{
public:
    using Request::Request;

    AssignMaterializedViewRequest(
        Stream && mv_,
        NodeID assign_node_,
        NodeID worker_node_,
        const NodeUUID & worker_node_uuid_,
        NodeID reassign_from_,
        uint16_t request_version_)
        : Request(request_version_), request_data(std::move(mv_), assign_node_, worker_node_, worker_node_uuid_, reassign_from_)
    {
    }

    protocol::AssignMaterializedViewRequestData & data() override { return request_data; }

    const protocol::AssignMaterializedViewRequestData & data() const override { return request_data; }

private:
    protocol::AssignMaterializedViewRequestData request_data;
};

using AssignMaterializedViewRequestPtr = std::shared_ptr<AssignMaterializedViewRequest>;
}
