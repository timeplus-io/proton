#pragma once

#include <Cluster/Protocol/DeleteStoragePolicyRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct DeleteStoragePolicyRequest final : public Request
{
public:
    using Request::Request;

    DeleteStoragePolicyRequest(std::string && name, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(name), initiator_, timeout_ms_)
    {
    }

    protocol::DeleteStoragePolicyRequestData & data() override { return request_data; }

    const protocol::DeleteStoragePolicyRequestData & data() const override { return request_data; }

private:
    protocol::DeleteStoragePolicyRequestData request_data;
};

using DeleteStoragePolicyRequestPtr = std::shared_ptr<DeleteStoragePolicyRequest>;
}
