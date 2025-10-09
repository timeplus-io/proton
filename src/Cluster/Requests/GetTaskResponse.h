#pragma once

#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/GetTaskResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct GetTaskResponse final : public Response
{
public:
    using Response::Response;

    GetTaskResponse(protocol::TaskDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    GetTaskResponse(protocol::TaskDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::TaskDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    GetTaskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetTaskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetTaskResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetTaskResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::GetTaskResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetTaskResponseData response_data;
};

using GetTaskResponsePtr = std::shared_ptr<GetTaskResponse>;
}
