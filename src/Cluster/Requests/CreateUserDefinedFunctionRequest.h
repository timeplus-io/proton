#pragma once

#include <Cluster/Protocol/CreateUserDefinedFunctionRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Create a udf
struct CreateUserDefinedFunctionRequest final : public Request
{
public:
    using Request::Request;

    CreateUserDefinedFunctionRequest(
        protocol::UserDefinedFunctionDescriptor && desc_,
        cluster::protocol::ExistsOperation exists_op,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(protocol::CreateUserDefinedFunctionRequestData(std::move(desc_), exists_op, initiator_, timeout_ms_))
    {
    }

    protocol::CreateUserDefinedFunctionRequestData & data() override { return request_data; }

    const protocol::CreateUserDefinedFunctionRequestData & data() const override { return request_data; }

private:
    protocol::CreateUserDefinedFunctionRequestData request_data;
};

using CreateUserDefinedFunctionRequestPtr = std::shared_ptr<CreateUserDefinedFunctionRequest>;
}
