#pragma once

#include <Cluster/Protocol/ListUserDefinedFunctionsRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a udf by name or all udfs if function name is empty
struct ListUserDefinedFunctionsRequest final : public Request
{
public:
    using Request::Request;

    ListUserDefinedFunctionsRequest(
        std::string && func_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(std::move(func_name_), initiator_, consistent_read_, timeout_ms_)
    {
    }

    ListUserDefinedFunctionsRequest(
        const std::string & func_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(func_name_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListUserDefinedFunctionsRequestData & data() override { return request_data; }

    const protocol::ListUserDefinedFunctionsRequestData & data() const override { return request_data; }

private:
    protocol::ListUserDefinedFunctionsRequestData request_data;
};

using ListUserDefinedFunctionsRequestPtr = std::shared_ptr<ListUserDefinedFunctionsRequest>;
}
