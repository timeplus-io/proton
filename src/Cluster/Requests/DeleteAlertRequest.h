#pragma once

#include <Cluster/Protocol/DeleteAlertRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Delete a format schema
struct DeleteAlertRequest final : public Request
{
public:
    using Request::Request;

    DeleteAlertRequest(
        std::string && ns, std::string && name, DB::UUID && id, cluster::NodeID initiator, int64_t timeout_ms, uint16_t request_version)
        : Request(request_version), request_data(std::move(ns), std::move(name), std::move(id), initiator, timeout_ms)
    {
    }

    DeleteAlertRequest(
        const std::string & ns,
        const std::string & name,
        const DB::UUID & id,
        cluster::NodeID initiator,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(ns, name, id, initiator, timeout_ms)
    {
    }

    protocol::DeleteAlertRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::DeleteAlertRequestData & data() const override { return request_data; }

private:
    protocol::DeleteAlertRequestData request_data;
};

using DeleteAlertRequestPtr = std::shared_ptr<DeleteAlertRequest>;
}
