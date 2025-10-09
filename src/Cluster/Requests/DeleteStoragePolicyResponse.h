#pragma once

#include <Cluster/Protocol/DeleteStoragePolicyResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct DeleteStoragePolicyResponse final : public Response
{
public:
    using Response::Response;

    DeleteStoragePolicyResponse(cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(leader_replica_, sn_)
    {
    }

    DeleteStoragePolicyResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    DeleteStoragePolicyResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    DeleteStoragePolicyResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::DeleteStoragePolicyResponseData & data() override { return response_data; }

    const protocol::DeleteStoragePolicyResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::DeleteStoragePolicyResponseData response_data;
};

using DeleteStoragePolicyResponsePtr = std::shared_ptr<DeleteStoragePolicyResponse>;
}
