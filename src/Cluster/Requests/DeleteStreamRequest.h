#pragma once

#include <Cluster/Protocol/DeleteStreamRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Delete a stream
struct DeleteStreamRequest final : public Request
{
public:
    using Request::Request;

    DeleteStreamRequest(
        std::string && ns_,
        std::string && stream_,
        const StreamID & stream_id_,
        std::string && sql_ddl_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(std::move(ns_), std::move(stream_), stream_id_, std::move(sql_ddl_), initiator_, timeout_ms_)
    {
    }

    DeleteStreamRequest(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & stream_id_,
        std::string && sql_ddl_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(ns_, stream_, stream_id_, std::move(sql_ddl_), initiator_, timeout_ms_)
    {
    }

    protocol::DeleteStreamRequestData & data() override { return request_data; }

    const protocol::DeleteStreamRequestData & data() const override { return request_data; }

private:
    protocol::DeleteStreamRequestData request_data;
};

using DeleteStreamRequestPtr = std::shared_ptr<DeleteStreamRequest>;
}
