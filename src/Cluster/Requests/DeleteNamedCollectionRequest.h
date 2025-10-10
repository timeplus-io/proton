#pragma once

#include <Cluster/Protocol/DeleteNamedCollectionRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct DeleteNamedCollectionRequest final : public Request
{
public:
    using Request::Request;

    DeleteNamedCollectionRequest(std::string key, cluster::NodeID initiator, int64_t timeout_ms, uint16_t request_version)
        : Request(request_version), request_data(std::move(key), initiator, timeout_ms)
    {
    }

    protocol::DeleteNamedCollectionRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::DeleteNamedCollectionRequestData & data() const override { return request_data; }

private:
    protocol::DeleteNamedCollectionRequestData request_data;
};

using DeleteNamedCollectionRequestPtr = std::shared_ptr<DeleteNamedCollectionRequest>;
}
