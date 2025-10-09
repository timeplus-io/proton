#pragma once

#include <Cluster/Protocol/GetAlertRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Get a format schema by name and format or all format schemas if schema name and format is empty
struct GetAlertRequest final : public Request
{
public:
    using Request::Request;

    GetAlertRequest(
        std::string && ns,
        std::string && name,
        uint64_t versions_requested,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(ns), std::move(name), versions_requested, initiator, consistent_read, timeout_ms)
    {
    }

    GetAlertRequest(
        const std::string & ns,
        const std::string & name,
        uint64_t versions_requested,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(ns, name, versions_requested, initiator, consistent_read, timeout_ms)
    {
    }

    protocol::GetAlertRequestData & data() override { return request_data; }

    const protocol::GetAlertRequestData & data() const override { return request_data; }

private:
    protocol::GetAlertRequestData request_data;
};

using GetAlertRequestPtr = std::shared_ptr<GetAlertRequest>;
}
