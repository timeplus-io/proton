#pragma once

#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/GetAlertResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct GetAlertResponse final : public Response
{
public:
    using Response::Response;

    GetAlertResponse(protocol::AlertDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    GetAlertResponse(protocol::AlertDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::AlertDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    GetAlertResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetAlertResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetAlertResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetAlertResponseData & data() override { return response_data; }

    const protocol::GetAlertResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetAlertResponseData response_data;
};

using GetAlertResponsePtr = std::shared_ptr<GetAlertResponse>;
}
