#pragma once

#include <Cluster/Protocol/CreateDatabaseRequestData.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{

struct CreateDatabaseRequest final : public Request
{
public:
    using Request::Request;

    CreateDatabaseRequest(protocol::DatabaseDescriptorPtr desc, cluster::NodeID initiator_, int64_t timeout_ms_, uint16_t request_version_)
        : Request(request_version_), request_data(std::move(desc), initiator_, timeout_ms_)
    {
    }

    protocol::CreateDatabaseRequestData & data() override { return request_data; }

    const protocol::CreateDatabaseRequestData & data() const override { return request_data; }

private:
    protocol::CreateDatabaseRequestData request_data;
};

using CreateDatabaseRequestPtr = std::shared_ptr<CreateDatabaseRequest>;
}
