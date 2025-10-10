#pragma once

#include <Cluster/Protocol/CreateNamedCollectionResponseData.h>
#include <Cluster/Requests/Response.h>


namespace cluster
{
struct CreateNamedCollectionResponse final : public Response
{
public:
    using Response::Response;

    CreateNamedCollectionResponse(NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    CreateNamedCollectionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateNamedCollectionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateNamedCollectionResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::CreateNamedCollectionResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::CreateNamedCollectionResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateNamedCollectionResponseData response_data;
};

using CreateNamedCollectionResponsePtr = std::shared_ptr<CreateNamedCollectionResponse>;
}
