#pragma once

#include <Cluster/Protocol/DatabaseDescriptor.h>
#include <Cluster/Protocol/ListDatabasesResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct ListDatabasesResponse final : public Response
{
public:
    using Response::Response;

    ListDatabasesResponse(
        protocol::DatabaseDescriptorPtrs && databases, cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(databases), leader_replica_, sn_)
    {
    }

    ListDatabasesResponse(protocol::DatabaseDescriptorPtr && database, cluster::NodeID leader_replica_, int64_t sn_, int16_t data_version_)
        : Response(data_version_), response_data(protocol::DatabaseDescriptorPtrs{std::move(database)}, leader_replica_, sn_)
    {
    }

    ListDatabasesResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListDatabasesResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListDatabasesResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListDatabasesResponseData & data() override { return response_data; }

    const protocol::ListDatabasesResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListDatabasesResponseData response_data;
};

using ListDatabasesResponsePtr = std::shared_ptr<ListDatabasesResponse>;
}
