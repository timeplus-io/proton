#pragma once

#include <Cluster/Protocol/ChangeLogLevelRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// SYSTEM SET LOG LEVEL
struct ChangeLogLevelRequest final : public Request
{
public:
    using Request::Request;

    ChangeLogLevelRequest(
        std::string && log_level, std::string && logger_name, NodeID initiator_, int64_t timeout_ms, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(log_level), std::move(logger_name), initiator_, timeout_ms)
    {
    }

    protocol::ChangeLogLevelRequestData & data() override { return request_data; }

    const protocol::ChangeLogLevelRequestData & data() const override { return request_data; }

private:
    protocol::ChangeLogLevelRequestData request_data;
};

using ChangeLogLevelRequestPtr = std::shared_ptr<ChangeLogLevelRequest>;
}
