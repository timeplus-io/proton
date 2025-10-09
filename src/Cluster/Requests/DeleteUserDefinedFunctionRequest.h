#pragma once

#include <Cluster/Protocol/DeleteUserDefinedFunctionRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Delete a udf
struct DeleteUserDefinedFunctionRequest final : public Request
{
public:
    using Request::Request;

    DeleteUserDefinedFunctionRequest(std::string && func_name_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(func_name_), initiator_, timeout_ms_)
    {
    }

    DeleteUserDefinedFunctionRequest(
        const std::string & func_name_, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(func_name_), initiator_, timeout_ms_)
    {
    }

    protocol::DeleteUserDefinedFunctionRequestData & data() override { return request_data; }

    const protocol::DeleteUserDefinedFunctionRequestData & data() const override { return request_data; }

private:
    protocol::DeleteUserDefinedFunctionRequestData request_data;
};

using DeleteUserDefinedFunctionRequestPtr = std::shared_ptr<DeleteUserDefinedFunctionRequest>;
}
