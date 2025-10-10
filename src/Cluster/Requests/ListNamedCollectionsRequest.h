#pragma once

#include <Cluster/Protocol/ListNamedCollectionsRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct ListNamedCollectionsRequest final : public Request
{
public:
    using Request::Request;

    ListNamedCollectionsRequest(cluster::NodeID initiator, bool consistent_read, int64_t timeout_ms, uint16_t request_version)
        : Request(request_version), request_data(initiator, consistent_read, timeout_ms)
    {
    }

    protocol::ListNamedCollectionsRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::ListNamedCollectionsRequestData & data() const override { return request_data; }

private:
    protocol::ListNamedCollectionsRequestData request_data;
};

using ListNamedCollectionsRequestPtr = std::shared_ptr<ListNamedCollectionsRequest>;
}
