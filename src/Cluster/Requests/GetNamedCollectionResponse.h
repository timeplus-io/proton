#pragma once

#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/GetNamedCollectionResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct GetNamedCollectionResponse final : public Response
{
public:
    using Response::Response;

    GetNamedCollectionResponse(
        protocol::NamedCollectionDescriptorPtrs named_collections, cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(named_collections), replica_leader_, sn_)
    {
    }

    GetNamedCollectionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    GetNamedCollectionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    GetNamedCollectionResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::GetNamedCollectionResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::GetNamedCollectionResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::GetNamedCollectionResponseData response_data;
};

using GetNamedCollectionResponsePtr = std::shared_ptr<GetNamedCollectionResponse>;
}
