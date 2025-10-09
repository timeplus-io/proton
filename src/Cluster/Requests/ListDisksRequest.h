#pragma once

#include <Cluster/Protocol/ListDisksRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a format schema by name and format or all format schemas if schema name and format is empty
struct ListDisksRequest final : public Request
{
public:
    using Request::Request;

    ListDisksRequest(
        std::string && disk_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(std::move(disk_name_), initiator_, consistent_read_, timeout_ms_)
    {
    }

    ListDisksRequest(
        const std::string & disk_name_, cluster::NodeID initiator_, bool consistent_read_, int64_t timeout_ms_, uint16_t request_version)
        : Request(request_version), request_data(disk_name_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListDisksRequestData & data() override { return request_data; }

    const protocol::ListDisksRequestData & data() const override { return request_data; }

private:
    protocol::ListDisksRequestData request_data;
};

using ListDisksRequestPtr = std::shared_ptr<ListDisksRequest>;
}
