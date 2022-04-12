#pragma once

#include <Compression/CompressionInfo.h>
#include <NativeLog/Base/ByteVector.h>
#include <NativeLog/Common/Stream.h>

namespace nlog
{
struct CreateStreamRequest;

struct StreamDescription
{
public:
    ByteVector serialize();

    static StreamDescription deserialize(const char * data, size_t size);
    static StreamDescription from(std::string ns_, const CreateStreamRequest & req);

public:
    int32_t version = 0;

    StreamID id;
    std::string ns;
    std::string stream;
    int32_t shards;
    uint8_t replicas;
    bool compacted;

    DB::CompressionMethodByte codec;

    int32_t flush_ms;
    int32_t flush_messages;

    int64_t retention_ms;
    int64_t retention_bytes;

    int64_t create_timestamp_ms;
    int64_t last_modify_timestamp_ms;

    bool operator==(const StreamDescription & rhs) const
    {
        return version == rhs.version && id == rhs.id && ns == rhs.ns && stream == rhs.stream && shards == rhs.shards
            && replicas == rhs.replicas && compacted == rhs.compacted && codec == rhs.codec && flush_ms == rhs.flush_ms
            && flush_messages == rhs.flush_messages && retention_ms == rhs.retention_ms && retention_bytes == rhs.retention_bytes
            && create_timestamp_ms == rhs.create_timestamp_ms && last_modify_timestamp_ms == rhs.last_modify_timestamp_ms;
    }

    std::string string() const;
};
}
