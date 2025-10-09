#pragma once

#include <Cluster/Protocol/DeleteDiskRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{

struct DeleteDiskRequest final : public Request
{
public:
    using Request::Request;

    DeleteDiskRequest(const std::string && name, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(name), initiator_, timeout_ms_)
    {
    }

    protocol::DeleteDiskRequestData & data() override { return request_data; }

    const protocol::DeleteDiskRequestData & data() const override { return request_data; }

private:
    protocol::DeleteDiskRequestData request_data;
};

using DeleteDiskRequestPtr = std::shared_ptr<DeleteDiskRequest>;
}
