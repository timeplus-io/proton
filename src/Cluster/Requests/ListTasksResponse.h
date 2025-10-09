#pragma once

#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Protocol/ListTasksResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ListTasksResponse final : public Response
{
public:
    using Response::Response;

    ListTasksResponse(protocol::TaskDescriptorPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    ListTasksResponse(protocol::TaskDescriptorPtr && desc, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(protocol::TaskDescriptorPtrs{std::move(desc)}, replica_leader_, sn_)
    {
    }

    ListTasksResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListTasksResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListTasksResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListTasksResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::ListTasksResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListTasksResponseData response_data;
};

using ListTasksResponsePtr = std::shared_ptr<ListTasksResponse>;
}
