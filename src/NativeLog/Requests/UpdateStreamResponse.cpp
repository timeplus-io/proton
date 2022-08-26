#include "UpdateStreamResponse.h"

#include <IO/WriteHelpers.h>

namespace nlog
{
UpdateStreamResponse UpdateStreamResponse::from(
    const StreamID & id_, const std::string & ns_, const std::string & stream_, const StreamDescription & desc, int32_t api_version_)
{
    UpdateStreamResponse response{id_, ns_, stream_, api_version_};

    response.shards = desc.shards;
    response.replicas = desc.replicas;
    response.codec = desc.codec;
    response.flush_ms = desc.flush_ms;
    response.flush_messages = desc.flush_messages;
    response.retention_ms = desc.retention_ms;
    response.retention_bytes = desc.retention_bytes;
    response.compacted = desc.compacted;
    response.create_timestamp = desc.create_timestamp_ms;
    response.last_modify_timestamp = desc.last_modify_timestamp_ms;

    return response;
}

std::string UpdateStreamResponse::string() const
{
    return fmt::format(
        "id={} namespace={} stream={} shards={} replicas={} codec={} compacted={} version={} create_timestamp={} last_modify_timestamp={} "
        "{}",
        DB::toString(id),
        ns,
        stream,
        shards,
        replicas,
        static_cast<uint8_t>(codec),
        compacted,
        version,
        create_timestamp,
        last_modify_timestamp,
        CommonResponse::string());
}
}
