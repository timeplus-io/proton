#pragma once

#include <Cluster/Protocol/CreateFormatSchemaRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Create a format schema
struct CreateFormatSchemaRequest final : public Request
{
public:
    using Request::Request;

    CreateFormatSchemaRequest(
        const std::string & schema_name,
        const std::string & format,
        const std::string & schema_body,
        protocol::ExistsOperation exists_op,
        const std::string & executed_by,
        uint32_t data_version_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              protocol::FormatSchemaDescriptor{schema_name, format, schema_body, data_version_}, exists_op, executed_by, initiator_, timeout_ms_)
    {
    }

    protocol::CreateFormatSchemaRequestData & data() override { return request_data; }

    const protocol::CreateFormatSchemaRequestData & data() const override { return request_data; }

private:
    protocol::CreateFormatSchemaRequestData request_data;
};

using CreateFormatSchemaRequestPtr = std::shared_ptr<CreateFormatSchemaRequest>;
}
