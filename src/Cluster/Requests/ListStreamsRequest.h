#pragma once

#include <Cluster/Protocol/ListStreamsRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a stream by name or all streams if stream name is empty
struct ListStreamsRequest final : public Request
{
public:
    using Request::Request;

    ListStreamsRequest(
        std::string && ns_,
        std::string && stream_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(ns_), std::move(stream_), initiator_, consistent_read_, timeout_ms_)
    {
    }

    ListStreamsRequest(
        const std::string & ns_,
        const std::string & stream_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(ns_, stream_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListStreamsRequestData & data() override { return request_data; }

    const protocol::ListStreamsRequestData & data() const override { return request_data; }

private:
    protocol::ListStreamsRequestData request_data;
};

using ListStreamsRequestPtr = std::shared_ptr<ListStreamsRequest>;
}
