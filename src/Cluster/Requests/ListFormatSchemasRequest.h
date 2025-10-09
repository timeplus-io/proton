#pragma once

#include <Cluster/Protocol/ListFormatSchemasRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// List a format schema by name and format or all format schemas if schema name and format is empty
struct ListFormatSchemasRequest final : public Request
{
public:
    using Request::Request;

    ListFormatSchemasRequest(
        std::string && schema_name_,
        std::string && format_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(std::move(schema_name_), std::move(format_), consistent_read_, initiator_, timeout_ms_)
    {
    }

    ListFormatSchemasRequest(
        const std::string & schema_name_,
        const std::string & format_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(schema_name_, format_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::ListFormatSchemasRequestData & data() override { return request_data; }

    const protocol::ListFormatSchemasRequestData & data() const override { return request_data; }

private:
    protocol::ListFormatSchemasRequestData request_data;
};

using ListFormatSchemasRequestPtr = std::shared_ptr<ListFormatSchemasRequest>;
}
