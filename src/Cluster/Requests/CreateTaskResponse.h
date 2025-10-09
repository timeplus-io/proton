#pragma once

#include <Cluster/Protocol/CreateTaskResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct CreateTaskResponse final : public Response
{
public:
    using Response::Response;

    CreateTaskResponse(NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    CreateTaskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateTaskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateTaskResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::CreateTaskResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::CreateTaskResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateTaskResponseData response_data;
};

using CreateTaskResponsePtr = std::shared_ptr<CreateTaskResponse>;
}
