#pragma once

#include <Cluster/Common/Nulls.h>
/// #include <Cluster/Common/Placement.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <base/ClockUtils.h>

namespace cluster::protocol
{
struct UpdateStreamSettingsRequestData final : public ProtocolData
{
    /// Used for deserialization
    UpdateStreamSettingsRequestData() = default;

    UpdateStreamSettingsRequestData(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & id_,
        uint32_t version_before_update_,
        std::unordered_map<std::string, int32_t> && flush_settings_,
        std::unordered_map<std::string, int64_t> && retention_settings_,
        std::vector<std::string> && alter_commands_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : version_before_update(version_before_update_)
        , alter_commands(std::move(alter_commands_))
        , stream(ns_, stream_, id_)
        , flush_settings(std::move(flush_settings_))
        , retention_settings(std::move(retention_settings_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    UpdateStreamSettingsRequestData(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & id_,
        uint32_t version_before_update_,
        std::string && sql_def_,
        std::vector<std::string> && alter_commands_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : version_before_update(version_before_update_)
        , alter_commands(std::move(alter_commands_))
        , stream(ns_, stream_, id_)
        , sql_def(std::move(sql_def_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    UpdateStreamSettingsRequestData(
        const std::string & ns_,
        const std::string & stream_,
        const StreamID & id_,
        uint32_t version_before_update_,
        std::string && sql_def_,
        std::unordered_map<std::string, int32_t> && flush_settings_,
        std::unordered_map<std::string, int64_t> && retention_settings_,
        std::vector<std::string> && alter_commands_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : version_before_update(version_before_update_)
        , alter_commands(std::move(alter_commands_))
        , stream(ns_, stream_, id_)
        , sql_def(std::move(sql_def_))
        , flush_settings(std::move(flush_settings_))
        , retention_settings(std::move(retention_settings_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::AlterStreamSettings; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 3}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    uint32_t version_before_update{Nulls::NullVersion};

    /// flags are deprecated in this struct
    /// flags for internal use to mimic transaction
    /// bits : 0-55, for application code encoding
    /// bits : 56-63 for type
    uint64_t flags = 0x0;

    /// DDL SQL which triggers the C/U/D mutate request
    std::vector<std::string> alter_commands;

    Stream stream;

    /// New stream def
    std::string sql_def;

    /// These are nativelog settings
    std::unordered_map<std::string, int32_t> flush_settings;
    std::unordered_map<std::string, int64_t> retention_settings;

    /// Added in v2 schema
    std::string modified_by;
    int64_t last_modified = DB::UTCMilliseconds::now();

    cluster::NodeID initiator = 0x0;
    int64_t timeout_ms = 0;
};

using UpdateStreamSettingsRequestDataPtr = std::shared_ptr<UpdateStreamSettingsRequestData>;
}
