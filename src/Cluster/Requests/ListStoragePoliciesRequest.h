#pragma once

#include <Cluster/Protocol/ListStoragePoliciesRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a storage policy by name or all storage policies if storage policy name is empty
struct ListStoragePoliciesRequest final : public Request
{
public:
    using Request::Request;

    ListStoragePoliciesRequest(
        std::string && name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(std::move(name_), initiator_, consistent_read_, timeout_ms_)
    {
    }

    ListStoragePoliciesRequest(
        const std::string & name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(name_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListStoragePoliciesRequestData & data() override { return request_data; }

    const protocol::ListStoragePoliciesRequestData & data() const override { return request_data; }

private:
    protocol::ListStoragePoliciesRequestData request_data;
};

using ListStoragePoliciesRequestPtr = std::shared_ptr<ListStoragePoliciesRequest>;
}
