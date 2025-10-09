#pragma once

#include <Cluster/Protocol/GetStreamRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a stream by name or all streams if stream name is empty
struct GetStreamRequest final : public Request
{
public:
    using Request::Request;

    GetStreamRequest(
        std::string && ns_,
        std::string && stream_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version)
        , request_data(std::move(ns_), std::move(stream_), versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    GetStreamRequest(
        const std::string & ns_,
        const std::string & stream_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(ns_, stream_, versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::GetStreamRequestData & data() override { return request_data; }

    const protocol::GetStreamRequestData & data() const override { return request_data; }

private:
    protocol::GetStreamRequestData request_data;
};

using GetStreamRequestPtr = std::shared_ptr<GetStreamRequest>;
}
