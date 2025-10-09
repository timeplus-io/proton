#pragma once

#include <Cluster/Protocol/RenameStreamRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct RenameStreamRequest final : public Request
{
public:
    using Request::Request;

    RenameStreamRequest(
        std::string && ns_,
        std::string && stream_,
        std::string && new_stream_,
        std::string && sql_ddl_,
        std::string && modified_by,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              std::move(ns_),
              std::move(stream_),
              std::move(new_stream_),
              std::move(sql_ddl_),
              std::move(modified_by),
              initiator_,
              timeout_ms_)
    {
    }

    protocol::RenameStreamRequestData & data() override { return request_data; }

    const protocol::RenameStreamRequestData & data() const override { return request_data; }

private:
    protocol::RenameStreamRequestData request_data;
};

using RenameStreamRequestPtr = std::shared_ptr<RenameStreamRequest>;
}
