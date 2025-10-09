#pragma once

#include <Cluster/Protocol/UpdateStreamSchemaRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct UpdateStreamSchemaRequest final : public Request
{
public:
    using Request::Request;

    UpdateStreamSchemaRequest(
        std::string database_,
        std::string table_,
        StreamID id_,
        std::vector<std::string> alter_commands_,
        std::string sql_def_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint32_t version_before_update_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              std::move(database_),
              std::move(table_),
              id_,
              std::move(alter_commands_),
              std::move(sql_def_),
              initiator_,
              timeout_ms_,
              version_before_update_)
    {
    }

    protocol::UpdateStreamSchemaRequestData & data() override { return request_data; }

    const protocol::UpdateStreamSchemaRequestData & data() const override { return request_data; }

private:
    protocol::UpdateStreamSchemaRequestData request_data;
};

using UpdateStreamSchemaRequestPtr = std::shared_ptr<UpdateStreamSchemaRequest>;
}
