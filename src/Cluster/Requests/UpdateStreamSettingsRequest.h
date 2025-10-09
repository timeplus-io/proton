#pragma once

#include <Cluster/Protocol/UpdateStreamSettingsRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
struct UpdateStreamSettingsRequest final : public Request
{
public:
    using Request::Request;

    UpdateStreamSettingsRequest(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & id_,
        uint32_t version_before_update_,
        std::string && sql_def_,
        std::unordered_map<std::string, int32_t> flush_settings_,
        std::unordered_map<std::string, int64_t> retention_settings_,
        std::vector<std::string> && alter_commands_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              ns_,
              stream_,
              id_,
              version_before_update_,
              std::move(sql_def_),
              std::move(flush_settings_),
              std::move(retention_settings_),
              std::move(alter_commands_),
              initiator_,
              timeout_ms_)
    {
    }

    protocol::UpdateStreamSettingsRequestData & data() override { return request_data; }

    const protocol::UpdateStreamSettingsRequestData & data() const override { return request_data; }

private:
    protocol::UpdateStreamSettingsRequestData request_data;
};

using UpdateStreamSettingsRequestPtr = std::shared_ptr<UpdateStreamSettingsRequest>;
}
