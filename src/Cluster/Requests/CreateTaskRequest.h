#pragma once

#include <Cluster/Protocol/CreateTaskRequestData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct CreateTaskRequest final : public Request
{
public:
    using Request::Request;

    CreateTaskRequest(
        protocol::TaskDescriptor desc,
        protocol::ExistsOperation exists_op,
        const std::string & created_by,
        cluster::NodeID initiator,
        int64_t timeout_ms,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(std::move(desc), exists_op, created_by, initiator, timeout_ms)
    {
    }

    protocol::CreateTaskRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::CreateTaskRequestData & data() const override { return request_data; }

private:
    protocol::CreateTaskRequestData request_data;
};

using CreateTaskRequestPtr = std::shared_ptr<CreateTaskRequest>;
}
