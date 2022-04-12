#include "CreateStreamResponse.h"
#include "CreateStreamRequest.h"

#include <IO/WriteHelpers.h>

namespace nlog
{
CreateStreamResponse CreateStreamResponse::from(
    const StreamID & id_,
    const std::string & ns_,
    uint16_t version_,
    int64_t create_timestamp_,
    int64_t last_modify_timestamp_,
    const CreateStreamRequest & request,
    int32_t api_version_)
{
    CreateStreamResponse response(id_, ns_, version_, create_timestamp_, last_modify_timestamp_, api_version_);

    response.stream = request.stream;
    response.shards = request.shards;
    response.replicas = request.replicas;
    response.codec = request.codec;
    response.flush_ms = request.flush_ms;
    response.flush_messages = request.flush_messages;
    response.retention_ms = request.retention_ms;
    response.retention_bytes = request.retention_bytes;
    response.compacted = request.compacted;

    return response;
}

std::string CreateStreamResponse::string() const
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
