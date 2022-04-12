#pragma once

#include "CommonResponse.h"

#include <Compression/CompressionInfo.h>
#include <NativeLog/Common/Stream.h>

namespace nlog
{
struct CreateStreamRequest;

struct CreateStreamResponse final : public CommonResponse
{
    static CreateStreamResponse from(
        const StreamID & id_,
        const std::string & ns_,
        uint16_t version_,
        int64_t create_timestamp_,
        int64_t last_modify_timestamp_,
        const CreateStreamRequest & request,
        int32_t api_version_);

public:
    CreateStreamResponse(
        const StreamID & id_,
        const std::string & ns_,
        uint16_t version_,
        int64_t create_timestamp_,
        int64_t last_modify_timestamp_,
        int32_t api_version_ = 0)
        : CommonResponse(api_version_)
        , id(id_)
        , ns(ns_)
        , version(version_)
        , create_timestamp(create_timestamp_)
        , last_modify_timestamp(last_modify_timestamp_)
    {
    }

    StreamID id;
    std::string ns;

    std::string stream;
    uint32_t shards;
    uint8_t replicas;
    DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE;

    int32_t flush_ms;
    int32_t flush_messages;

    int64_t retention_ms;
    int64_t retention_bytes;

    bool compacted = false;

    uint16_t version;
    int64_t create_timestamp; /// Milliseconds since utc
    int64_t last_modify_timestamp; /// Milliseconds since utc

    std::string string() const override;
};

}
