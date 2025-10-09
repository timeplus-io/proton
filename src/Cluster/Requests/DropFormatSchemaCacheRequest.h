#pragma once

#include <Cluster/Protocol/DropFormatSchemaCacheRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// SYSTEM <Command>
struct DropFormatSchemaCacheRequest final : public Request
{
public:
    using Request::Request;

    DropFormatSchemaCacheRequest(
        std::unordered_set<std::string> && caches_to_drop, NodeID initiator_, int64_t timeout_ms, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(caches_to_drop), initiator_, timeout_ms)
    {
    }

    protocol::DropFormatSchemaCacheRequestData & data() override { return request_data; }

    const protocol::DropFormatSchemaCacheRequestData & data() const override { return request_data; }

private:
    protocol::DropFormatSchemaCacheRequestData request_data;
};

using DropFormatSchemaCacheRequestPtr = std::shared_ptr<DropFormatSchemaCacheRequest>;
}
