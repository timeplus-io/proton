#pragma once

#include <Cluster/Protocol/DeleteTaskRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct DeleteTaskRequest final : public Request
{
public:
    using Request::Request;

    DeleteTaskRequest(
        std::string && ns, std::string && name, DB::UUID && id, cluster::NodeID initiator, int64_t timeout_ms, uint16_t request_version)
        : Request(request_version), request_data(std::move(ns), std::move(name), std::move(id), initiator, timeout_ms)
    {
    }

    DeleteTaskRequest(
        const std::string & ns,
        const std::string & name,
        const DB::UUID & id,
        cluster::NodeID initiator,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(ns, name, id, initiator, timeout_ms)
    {
    }

    protocol::DeleteTaskRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::DeleteTaskRequestData & data() const override { return request_data; }

private:
    protocol::DeleteTaskRequestData request_data;
};

using DeleteTaskRequestPtr = std::shared_ptr<DeleteTaskRequest>;
}
