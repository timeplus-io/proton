#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Protocol/ChangeLogLevelResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
/// SYSTEM SET LOG LEVEL response
struct ChangeLogLevelResponse final : public Response
{
public:
    using Response::Response;

    explicit ChangeLogLevelResponse(Error && err_, uint16_t response_version_) : Response(response_version_), response_data(std::move(err_))
    {
    }

    ChangeLogLevelResponse(int32_t error_code, std::string_view error_message, uint16_t response_version_)
        : Response(response_version_), response_data(error_code, std::string(error_message))
    {
    }

    ChangeLogLevelResponse(int32_t error_code, std::string && error_message, uint16_t response_version_)
        : Response(response_version_), response_data(error_code, std::move(error_message))
    {
    }

    ChangeLogLevelResponse(int64_t sn_, uint16_t response_version_) : Response(response_version_), response_data(sn_) { }

    ChangeLogLevelResponse(cluster::NodeID replica_leader_, int64_t sn_, uint16_t response_version_)
        : Response(response_version_), response_data(replica_leader_, sn_)
    {
    }

    protocol::ChangeLogLevelResponseData & data() override { return response_data; }

    const protocol::ChangeLogLevelResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }
    std::string errorString() const { return response_data.errorString(); }

private:
    protocol::ChangeLogLevelResponseData response_data;
};

using ChangeLogLevelResponsePtr = std::shared_ptr<ChangeLogLevelResponse>;
}
