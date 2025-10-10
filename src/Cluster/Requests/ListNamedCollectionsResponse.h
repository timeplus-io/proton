#pragma once

#include <Cluster/Protocol/ListNamedCollectionsResponseData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ListNamedCollectionsResponse final : public Response
{
public:
    using Response::Response;

    ListNamedCollectionsResponse(std::vector<std::string> keys, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(keys), replica_leader_, sn_)
    {
    }

    ListNamedCollectionsResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListNamedCollectionsResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListNamedCollectionsResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListNamedCollectionsResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::ListNamedCollectionsResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListNamedCollectionsResponseData response_data;
};

using ListNamedCollectionsResponsePtr = std::shared_ptr<ListNamedCollectionsResponse>;
}
