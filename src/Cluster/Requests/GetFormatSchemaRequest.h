#pragma once

#include <Cluster/Protocol/GetFormatSchemaRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Get a format schema by name and format or all format schemas if schema name and format is empty
struct GetFormatSchemaRequest final : public Request
{
public:
    using Request::Request;

    GetFormatSchemaRequest(
        std::string && schema_name_,
        std::string && format_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version)
        , request_data(std::move(schema_name_), std::move(format_), versions_requested_, consistent_read_, initiator_, timeout_ms_)
    {
    }

    GetFormatSchemaRequest(
        const std::string & schema_name_,
        const std::string & format_,
        uint64_t versions_requested_,
        cluster::NodeID initiator_,
        bool consistent_read_,
        int64_t timeout_ms_,
        uint16_t request_version)
        : Request(request_version), request_data(schema_name_, format_, versions_requested_, initiator_, consistent_read_, timeout_ms_)
    {
    }

    protocol::GetFormatSchemaRequestData & data() override { return request_data; }

    const protocol::GetFormatSchemaRequestData & data() const override { return request_data; }

private:
    protocol::GetFormatSchemaRequestData request_data;
};

using GetFormatSchemaRequestPtr = std::shared_ptr<GetFormatSchemaRequest>;
}
