#pragma once

/// Placement.h completely removed
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>
#include <Cluster/Protocol/StreamDescriptor.h>

namespace cluster::protocol
{
struct CreateStreamRequestData final : public ProtocolData
{
    /// Used for deserialization
    CreateStreamRequestData() = default;

    CreateStreamRequestData(
        std::string && sql_ddl_, StreamDescriptor && desc_, NodeID initiator_, int64_t timeout_ms_)
        : sql_ddl(std::move(sql_ddl_))
        , desc(std::move(desc_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::CreateStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 2}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override
    {
        return approximateSerializedSizeOf(sql_ddl) + desc.approximateSerializedSize();
    }

    std::string doString() const override;

    void setCodec(DB::CompressionMethodByte codec_) { desc.codec = codec_; }

    void setFlushInterval(uint32_t flush_ms_) { desc.flush_ms = flush_ms_; }

    void setFlushMessages(uint32_t flush_messages_) { desc.flush_messages = flush_messages_; }

    void setFlushBytes(uint32_t flush_bytes_) { desc.flush_bytes = flush_bytes_; }

    void setRetentionInterval(uint64_t retention_ms_) { desc.retention_ms = retention_ms_; }

    void setRetentionBytes(uint64_t retention_bytes_) { desc.retention_bytes = retention_bytes_; }

    void setCreatedBy(std::string && created_by_) { desc.created_by = std::move(created_by_); }

    void setLastModifiedBy(std::string && last_modified_by_) { desc.last_modified_by = std::move(last_modified_by_); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    /// Original DDL SQL
    std::string sql_ddl;

    StreamDescriptor desc;

    /// Added in v2 schema
    NodeID initiator = 0x0;
    int64_t timeout_ms = 0;
};

using CreateStreamRequestDataPtr = std::shared_ptr<CreateStreamRequestData>;
}
