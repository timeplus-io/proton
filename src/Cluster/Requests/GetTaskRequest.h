#pragma once

#include <Cluster/Protocol/GetTaskRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct GetTaskRequest final : public Request
{
public:
    using Request::Request;

    GetTaskRequest(
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

    GetTaskRequest(
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

    protocol::GetTaskRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::GetTaskRequestData & data() const override { return request_data; }

private:
    protocol::GetTaskRequestData request_data;
};

using GetTaskRequestPtr = std::shared_ptr<GetTaskRequest>;
}
