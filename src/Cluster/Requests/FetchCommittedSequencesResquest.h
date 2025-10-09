#pragma once

#include <Cluster/Protocol/FetchCommittedSequencesRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct FetchCommittedSequencesRequest final : public Request
{
public:
    using Request::Request;

    FetchCommittedSequencesRequest(
        const StreamIDShard & stream_shard_,
        int64_t last_sn_,
        uint64_t max_entries_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(stream_shard_, last_sn_, max_entries_, initiator_, timeout_ms_)
    {
    }

    protocol::FetchCommittedSequencesRequestData & data() override { return request_data; }

    const protocol::FetchCommittedSequencesRequestData & data() const override { return request_data; }

private:
    protocol::FetchCommittedSequencesRequestData request_data;
};

using FetchCommittedSequencesRequestPtr = std::shared_ptr<FetchCommittedSequencesRequest>;
}
