#pragma once

#include <Cluster/Protocol/DeleteDatabaseRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{

struct DeleteDatabaseRequest final : public Request
{
public:
    using Request::Request;

    DeleteDatabaseRequest(std::string name, std::string ddl_query, cluster::NodeID initiator_, int64_t timeout_ms, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(name), std::move(ddl_query), initiator_, timeout_ms)
    {
    }

    protocol::DeleteDatabaseRequestData & data() override { return request_data; }

    const protocol::DeleteDatabaseRequestData & data() const override { return request_data; }

private:
    protocol::DeleteDatabaseRequestData request_data;
};

using DeleteDatabaseRequestPtr = std::shared_ptr<DeleteDatabaseRequest>;
}
