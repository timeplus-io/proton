#pragma once

#include <Cluster/Protocol/CreateNamedCollectionRequestData.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct CreateNamedCollectionRequest final : public Request
{
public:
    using Request::Request;

    CreateNamedCollectionRequest(
        std::string collection_name,
        protocol::NamedCollectionDescriptorPtr collection,
        protocol::ExistsOperation exists_op,
        const std::string & created_by,
        cluster::NodeID initiator,
        int64_t timeout_ms,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(std::move(collection_name), std::move(collection), exists_op, created_by, initiator, timeout_ms)
    {
    }

    protocol::CreateNamedCollectionRequestData & data() override { return request_data; }
    [[nodiscard]] const protocol::CreateNamedCollectionRequestData & data() const override { return request_data; }

private:
    protocol::CreateNamedCollectionRequestData request_data;
};

using CreateNamedCollectionRequestPtr = std::shared_ptr<CreateNamedCollectionRequest>;
}
