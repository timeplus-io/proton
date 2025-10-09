#pragma once

#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/CreateStreamRequestData.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Create a stream
struct CreateStreamRequest final : public Request
{
public:
    using Request::Request;

    CreateStreamRequest(
        std::string && ns,
        std::string && stream,
        const StreamID & stream_id,
        uint32_t shards,
        uint32_t /*replicas*/, /// unused parameter kept for compatibility
        std::string && sql_def,
        std::string && sql_ddl_,
        NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              std::move(sql_ddl_),
              protocol::StreamDescriptor{std::move(ns), std::move(stream), stream_id, shards, std::move(sql_def), /*data_version_=*/1},
              initiator_,
              timeout_ms_)
    {
    }

    protocol::CreateStreamRequestData & data() override { return request_data; }

    const protocol::CreateStreamRequestData & data() const override { return request_data; }

    CreateStreamRequest & setCodec(DB::CompressionMethodByte codec_)
    {
        request_data.setCodec(codec_);
        return *this;
    }

    CreateStreamRequest & setFlushInterval(uint32_t flush_ms_)
    {
        request_data.setFlushInterval(flush_ms_);
        return *this;
    }

    CreateStreamRequest & setFlushMessages(uint32_t flush_messages_)
    {
        request_data.setFlushMessages(flush_messages_);
        return *this;
    }

    CreateStreamRequest & setFlushBytes(uint32_t flush_bytes_)
    {
        request_data.setFlushBytes(flush_bytes_);
        return *this;
    }

    CreateStreamRequest & setRetentionInterval(uint64_t retention_ms_)
    {
        request_data.setRetentionInterval(retention_ms_);
        return *this;
    }

    CreateStreamRequest & setRetentionBytes(uint64_t retention_bytes_)
    {
        request_data.setRetentionBytes(retention_bytes_);
        return *this;
    }

    CreateStreamRequest & setCreatedBy(std::string && created_by)
    {
        request_data.setCreatedBy(std::move(created_by));
        return *this;
    }

    CreateStreamRequest & setLastModifiedBy(std::string && last_modified_by)
    {
        request_data.setLastModifiedBy(std::move(last_modified_by));
        return *this;
    }

private:
    protocol::CreateStreamRequestData request_data;
};

using CreateStreamRequestPtr = std::shared_ptr<CreateStreamRequest>;
}
