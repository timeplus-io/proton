#pragma once

#include <Cluster/Protocol/DeleteTaskResponseData.h>
#include <Cluster/Requests/Response.h>


namespace cluster
{
struct DeleteTaskResponse final : public Response
{
public:
    using Response::Response;

    DeleteTaskResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    DeleteTaskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteTaskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    explicit DeleteTaskResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::DeleteTaskResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::DeleteTaskResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteTaskResponseData response_data;
};

using DeleteTaskResponsePtr = std::shared_ptr<DeleteTaskResponse>;
}
