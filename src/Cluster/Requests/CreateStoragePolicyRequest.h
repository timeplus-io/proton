#pragma once

#include <Cluster/Protocol/CreateStoragePolicyRequestData.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct CreateStoragePolicyRequest final : public Request
{
public:
    using Request::Request;

    CreateStoragePolicyRequest(
        protocol::StoragePolicyDescriptorPtr policy, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(policy), initiator_, timeout_ms_)
    {
    }

    protocol::CreateStoragePolicyRequestData & data() override { return request_data; }

    const protocol::CreateStoragePolicyRequestData & data() const override { return request_data; }

private:
    protocol::CreateStoragePolicyRequestData request_data;
};

using CreateStoragePolicyRequestPtr = std::shared_ptr<CreateStoragePolicyRequest>;
}
