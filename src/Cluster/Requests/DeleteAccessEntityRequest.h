#pragma once

#include <Cluster/Protocol/DeleteAccessEntityRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct DeleteAccessEntityRequest final : public Request
{
public:
    using Request::Request;

    DeleteAccessEntityRequest(const DB::UUID & id, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(id, initiator_, timeout_ms_)
    {
    }

    protocol::DeleteAccessEntityRequestData & data() override { return request_data; }

    const protocol::DeleteAccessEntityRequestData & data() const override { return request_data; }

private:
    protocol::DeleteAccessEntityRequestData request_data;
};

using DeleteAccessEntityRequestPtr = std::shared_ptr<DeleteAccessEntityRequest>;
}
