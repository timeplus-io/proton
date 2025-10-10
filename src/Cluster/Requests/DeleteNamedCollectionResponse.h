#pragma once

#include <Cluster/Protocol/DeleteNamedCollectionResponseData.h>
#include <Cluster/Requests/Response.h>


namespace cluster
{
struct DeleteNamedCollectionResponse final : public Response
{
public:
    using Response::Response;

    DeleteNamedCollectionResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteNamedCollectionResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteNamedCollectionResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteNamedCollectionResponse(Error && error, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(error))
    {
    }

    protocol::DeleteNamedCollectionResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::DeleteNamedCollectionResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteNamedCollectionResponseData response_data;
};

using DeleteNamedCollectionResponsePtr = std::shared_ptr<DeleteNamedCollectionResponse>;
}
