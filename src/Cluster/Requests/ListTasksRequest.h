#pragma once

#include <Cluster/Protocol/ListTasksRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct ListTasksRequest final : public Request
{
public:
    using Request::Request;

    ListTasksRequest(
        std::string && ns,
        std::string && name,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(ns), std::move(name), initiator, consistent_read, timeout_ms)
    {
    }

    ListTasksRequest(
        const std::string & ns,
        const std::string & name,
        cluster::NodeID initiator,
        bool consistent_read,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version), request_data(ns, name, initiator, consistent_read, timeout_ms)
    {
    }

    protocol::ListTasksRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::ListTasksRequestData & data() const override { return request_data; }

private:
    protocol::ListTasksRequestData request_data;
};

using ListTasksRequestPtr = std::shared_ptr<ListTasksRequest>;
}

