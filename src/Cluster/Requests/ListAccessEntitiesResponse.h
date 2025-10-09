#pragma once

#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/ListAccessEntitiesResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct ListAccessEntitiesResponse final : public Response
{
public:
    using Response::Response;

    ListAccessEntitiesResponse(
        protocol::AccessEntityDescriptionPtrs && descs, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(descs), replica_leader_, sn_)
    {
    }

    ListAccessEntitiesResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListAccessEntitiesResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListAccessEntitiesResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListAccessEntitiesResponseData & data() override { return response_data; }

    const protocol::ListAccessEntitiesResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListAccessEntitiesResponseData response_data;
};

using ListAccessEntitiesResponsePtr = std::shared_ptr<ListAccessEntitiesResponse>;
}
