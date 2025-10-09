#pragma once

#include <Cluster/Protocol/GetUserDefinedFunctionRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Get a udf by name or all udfs if function name is empty
struct GetUserDefinedFunctionRequest final : public Request
{
public:
    using Request::Request;

    GetUserDefinedFunctionRequest(
        std::string && func_name_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(func_name_), versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    GetUserDefinedFunctionRequest(
        const std::string & func_name_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::string{func_name_}, versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::GetUserDefinedFunctionRequestData & data() override { return request_data; }

    const protocol::GetUserDefinedFunctionRequestData & data() const override { return request_data; }

private:
    protocol::GetUserDefinedFunctionRequestData request_data;
};

using GetUserDefinedFunctionRequestPtr = std::shared_ptr<GetUserDefinedFunctionRequest>;
}
