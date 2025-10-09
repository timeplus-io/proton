#pragma once

#include <Cluster/Protocol/AlterTaskResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
class AlterTaskResponse final : public Response
{
public:
    using Response::Response;

    AlterTaskResponse(NodeID replica_leader_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(replica_leader_, sn_)
    {
    }

    AlterTaskResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    AlterTaskResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    AlterTaskResponse(Error && err, uint16_t data_version_) : Response(data_version_), response_data(std::move(err)) { }

    protocol::AlterTaskResponseData & data() override { return response_data; }

    [[nodiscard]] const protocol::AlterTaskResponseData & data() const override { return response_data; }

    [[nodiscard]] bool hasError() const noexcept override { return response_data.hasError(); }
    [[nodiscard]] const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::AlterTaskResponseData response_data;
};

using AlterTaskResponsePtr = std::shared_ptr<AlterTaskResponse>;
}
