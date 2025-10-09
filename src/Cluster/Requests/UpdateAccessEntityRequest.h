#pragma once

#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/UpdateAccessEntityRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct UpdateAccessEntityRequest final : public Request
{
public:
    using Request::Request;

    UpdateAccessEntityRequest(
        uint32_t version_before_update_,
        protocol::AccessEntityDescription new_entity_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(version_before_update_, std::move(new_entity_), initiator_, timeout_ms_)
    {
    }

    protocol::UpdateAccessEntityRequestData & data() override { return request_data; }

    const protocol::UpdateAccessEntityRequestData & data() const override { return request_data; }

private:
    protocol::UpdateAccessEntityRequestData request_data;
};

using UpdateAccessEntityRequestPtr = std::shared_ptr<UpdateAccessEntityRequest>;
}
