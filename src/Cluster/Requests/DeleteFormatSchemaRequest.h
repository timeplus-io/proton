#pragma once

#include <Cluster/Protocol/DeleteFormatSchemaRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Delete a format schema
struct DeleteFormatSchemaRequest final : public Request
{
public:
    using Request::Request;

    DeleteFormatSchemaRequest(
        std::string && schema_name_,
        std::string && format_,
        bool if_exists_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(std::move(schema_name_), std::move(format_), if_exists_, initiator_, timeout_ms_)
    {
    }

    DeleteFormatSchemaRequest(
        const std::string & schema_name_,
        const std::string & format_,
        bool if_exists_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_), request_data(schema_name_, format_, if_exists_, initiator_, timeout_ms_)
    {
    }

    protocol::DeleteFormatSchemaRequestData & data() override { return request_data; }

    const protocol::DeleteFormatSchemaRequestData & data() const override { return request_data; }

private:
    protocol::DeleteFormatSchemaRequestData request_data;
};

using DeleteFormatSchemaRequestPtr = std::shared_ptr<DeleteFormatSchemaRequest>;
}
