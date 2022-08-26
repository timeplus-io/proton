#pragma once

#include "CommonResponse.h"
#include "StreamDescription.h"

#include <Compression/CompressionInfo.h>
#include <NativeLog/Common/Stream.h>

namespace nlog
{
struct UpdateStreamResponse final : public CommonResponse
{
    static UpdateStreamResponse from(
        const StreamID & id_,
        const std::string & ns_,
        const std::string & stream_,
        const StreamDescription & desc,
        int32_t api_version_ = 0);

public:
    UpdateStreamResponse(const StreamID & id_, const std::string & ns_, const std::string & stream_, int32_t api_version_ = 0)
        : CommonResponse(api_version_), id(id_), ns(ns_), stream(stream_)
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
