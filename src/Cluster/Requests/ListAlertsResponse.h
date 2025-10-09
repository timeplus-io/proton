#pragma once

#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/ListAlertsResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ListAlertsResponse final : public Response
{
public:
    using Response::Response;

    ListAlertsResponse(protocol::AlertDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    ListAlertsResponse(protocol::AlertDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::AlertDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    ListAlertsResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListAlertsResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListAlertsResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListAlertsResponseData & data() override { return response_data; }

    const protocol::ListAlertsResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListAlertsResponseData response_data;
};

using ListAlertsResponsePtr = std::shared_ptr<ListAlertsResponse>;
}
