#pragma once

#include <Cluster/Protocol/ListAlertsRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a format schema by name and format or all format schemas if schema name and format is empty
struct ListAlertsRequest final : public Request
{
public:
    using Request::Request;

    ListAlertsRequest(
        std::string && ns,
        std::string && name,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(ns), std::move(name), initiator, consistent_read, timeout_ms)
    {
    }

    ListAlertsRequest(
        const std::string & ns,
        const std::string & name,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(ns, name, initiator, consistent_read, timeout_ms)
    {
    }

    protocol::ListAlertsRequestData & data() override { return request_data; }

    const protocol::ListAlertsRequestData & data() const override { return request_data; }

private:
    protocol::ListAlertsRequestData request_data;
};

using ListAlertsRequestPtr = std::shared_ptr<ListAlertsRequest>;
}
