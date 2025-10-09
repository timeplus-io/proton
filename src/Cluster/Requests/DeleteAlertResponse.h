#pragma once

#include <Cluster/Protocol/DeleteAlertResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct DeleteAlertResponse final : public Response
{
public:
    using Response::Response;

    DeleteAlertResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteAlertResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteAlertResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteAlertResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error))
    {
    }

    protocol::DeleteAlertResponseData & data() override { return response_data; }

    const protocol::DeleteAlertResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteAlertResponseData response_data;
};

using DeleteAlertResponsePtr = std::shared_ptr<DeleteAlertResponse>;
}
