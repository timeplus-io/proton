#pragma once

#include <Cluster/Protocol/TruncateStreamRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct TruncateStreamRequest final : public Request
{
public:
    using Request::Request;

    TruncateStreamRequest(
        std::string && ns_,
        std::string && stream_,
        const StreamID & id_,
        uint32_t shard_,
        int64_t sn_,
        std::string && sql_ddl_,
        uint16_t request_version_)
        : Request(request_version_), request_data(std::move(ns_), std::move(stream_), id_, shard_, sn_, std::move(sql_ddl_))
    {
    }

    protocol::TruncateStreamRequestData & data() override { return request_data; }

    const protocol::TruncateStreamRequestData & data() const override { return request_data; }

private:
    protocol::TruncateStreamRequestData request_data;
};

using TruncateStreamRequestPtr = std::shared_ptr<TruncateStreamRequest>;
}
