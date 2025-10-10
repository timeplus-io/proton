#pragma once

#include <Cluster/Protocol/GetNamedCollectionRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct GetNamedCollectionRequest final : public Request
{
public:
    using Request::Request;

    GetNamedCollectionRequest(
        std::string name,
        uint64_t versions_requested,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(name), versions_requested, initiator, consistent_read, timeout_ms)
    {
    }

    protocol::GetNamedCollectionRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::GetNamedCollectionRequestData & data() const override { return request_data; }

private:
    protocol::GetNamedCollectionRequestData request_data;
};

using GetNamedCollectionRequestPtr = std::shared_ptr<GetNamedCollectionRequest>;
}
