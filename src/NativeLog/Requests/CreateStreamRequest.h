#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/Stream.h>
#include <Compression/CompressionInfo.h>

#include <string>


namespace nlog
{
/// Create a stream
struct CreateStreamRequest final : public CommonRequest
{
    CreateStreamRequest(
        std::string stream_,
        const StreamID & stream_id_,
        int32_t shards_,
        uint8_t replicas_,
        int16_t api_version_ = 0)
        : CommonRequest(api_version_)
        , stream(std::move(stream_))
        , stream_id(stream_id_)
        , shards(shards_)
        , replicas(replicas_)
    {
    }

public:
    CreateStreamRequest & setCodec(DB::CompressionMethodByte codec_) { codec = codec_; return *this; }
    CreateStreamRequest & setFlushInterval(int32_t flush_ms_) { flush_ms = flush_ms_; return *this; }
    CreateStreamRequest & setFlushMessages(int32_t flush_messages_) { flush_messages = flush_messages_; return *this; }
    CreateStreamRequest & setRetentionInterval(int32_t retention_ms_) { retention_ms = retention_ms_; return *this; }
    CreateStreamRequest & setRetentionBytes(int32_t retention_bytes_) { retention_bytes = retention_bytes_; return *this; }
    CreateStreamRequest & setCompacted(bool compacted_) { compacted = compacted_; return *this; }

    std::string stream;
    StreamID stream_id;
    int32_t shards = 1;
    uint8_t replicas = 1;
    DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE;

    /// Flush settings: per interval or per messages whichever reaches first
    int32_t flush_ms = 5000;
    int32_t flush_messages = 1024 * 1024 * 4;

    /// Retention settings for `delete` policy, per interval or per data volume whichever reaches first
    int64_t retention_ms = -1; /// Data old than this interval will be deleted
    int64_t retention_bytes = -1; /// When data volume reaches this threshold, old data will be deleted

    /// When compacted is `true`, retention policy is `compact`, otherwise it's `delete`
    bool compacted = false;
};
}
