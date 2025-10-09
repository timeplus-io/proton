#pragma once

#include <Cluster/Protocol/ListStoragePoliciesResponseData.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ListStoragePoliciesResponse final : public Response
{
public:
    using Response::Response;

    ListStoragePoliciesResponse(
        protocol::StoragePolicyDescriptorPtrs && policies, cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(policies), leader_replica_, sn_)
    {
    }

    ListStoragePoliciesResponse(
        protocol::StoragePolicyDescriptorPtr && policy, cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::StoragePolicyDescriptorPtrs{std::move(policy)}, leader_replica_, sn_)
    {
    }

    ListStoragePoliciesResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListStoragePoliciesResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListStoragePoliciesResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListStoragePoliciesResponseData & data() override { return response_data; }

    const protocol::ListStoragePoliciesResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListStoragePoliciesResponseData response_data;
};

using ListStoragePoliciesResponsePtr = std::shared_ptr<ListStoragePoliciesResponse>;
}
