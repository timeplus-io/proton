#pragma once

#include <Cluster/Protocol/CreateDiskRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{

struct CreateDiskRequest final : public Request
{
public:
    using Request::Request;

    CreateDiskRequest(protocol::DiskDescriptorPtr disk, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(disk), initiator_, timeout_ms_)
    {
    }

    protocol::CreateDiskRequestData & data() override { return request_data; }

    const protocol::CreateDiskRequestData & data() const override { return request_data; }

private:
    protocol::CreateDiskRequestData request_data;
};

using CreateDiskRequestPtr = std::shared_ptr<CreateDiskRequest>;
}
