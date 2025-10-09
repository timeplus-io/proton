#pragma once

#include <Cluster/Protocol/ListDatabasesRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct ListDatabasesRequest final : public Request
{
public:
    using Request::Request;

    ListDatabasesRequest(cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListDatabasesRequestData & data() override { return request_data; }

    const protocol::ListDatabasesRequestData & data() const override { return request_data; }

private:
    protocol::ListDatabasesRequestData request_data;
};

using ListDatabasesRequestPtr = std::shared_ptr<ListDatabasesRequest>;
}
