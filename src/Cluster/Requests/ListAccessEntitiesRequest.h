#pragma once

#include <Cluster/Protocol/ListAccessEntitiesRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct ListAccessEntitiesRequest final : public Request
{
public:
    using Request::Request;

    ListAccessEntitiesRequest(cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(consistent_read_, initiator_, timeout_ms_)
    {
    }

    protocol::ListAccessEntitiesRequestData & data() override { return request_data; }

    const protocol::ListAccessEntitiesRequestData & data() const override { return request_data; }

private:
    protocol::ListAccessEntitiesRequestData request_data;
};

using ListAccessEntitiesRequestPtr = std::shared_ptr<ListAccessEntitiesRequest>;
}
