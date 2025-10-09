#pragma once

#include <Cluster/Protocol/CreateStoragePolicyResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct CreateStoragePolicyResponse final : public Response
{
public:
    using Response::Response;

    CreateStoragePolicyResponse(cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(leader_replica_, sn_)
    {
    }

    CreateStoragePolicyResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    CreateStoragePolicyResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    CreateStoragePolicyResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::CreateStoragePolicyResponseData & data() override { return response_data; }

    const protocol::CreateStoragePolicyResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::CreateStoragePolicyResponseData response_data;
};

using CreateStoragePolicyResponsePtr = std::shared_ptr<CreateStoragePolicyResponse>;
}
