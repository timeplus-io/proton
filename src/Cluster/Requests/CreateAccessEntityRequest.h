#pragma once

#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/CreateAccessEntityRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct CreateAccessEntityRequest final : public Request
{
public:
    using Request::Request;

    CreateAccessEntityRequest(
        protocol::AccessEntityDescription desc,
        bool replace_if_exists,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(std::move(desc), replace_if_exists, initiator_, timeout_ms_)
    {
    }

    protocol::CreateAccessEntityRequestData & data() override { return request_data; }

    const protocol::CreateAccessEntityRequestData & data() const override { return request_data; }

private:
    protocol::CreateAccessEntityRequestData request_data;
};

using CreateAccessEntityRequestPtr = std::shared_ptr<CreateAccessEntityRequest>;
}
