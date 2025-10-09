#pragma once

#include <Cluster/Protocol/GetDatabaseRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{

struct GetDatabaseRequest final : public Request
{
public:
    using Request::Request;

    GetDatabaseRequest(
        const std::string & name_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::string{name_}, versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    GetDatabaseRequest(
        std::string && name_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(name_), versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::GetDatabaseRequestData & data() override { return request_data; }

    const protocol::GetDatabaseRequestData & data() const override { return request_data; }

private:
    protocol::GetDatabaseRequestData request_data;
};

using GetDatabaseRequestPtr = std::shared_ptr<GetDatabaseRequest>;
}
