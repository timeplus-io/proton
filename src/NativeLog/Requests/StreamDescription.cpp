#include "StreamDescription.h"
#include "CreateStreamRequest.h"

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <NativeLog/Base/Utils.h>
#include <base/ClockUtils.h>

#include <fmt/format.h>

namespace nlog
{
ByteVector StreamDescription::serialize()
{
    size_t struct_size = sizeof(StreamDescription) + ns.size() + stream.size();
    ByteVector data{struct_size};
    data.resize(data.capacity());

    DB::WriteBufferFromVector wb{data};

    DB::writeIntBinary(version, wb);

    DB::writeBinary(id, wb);

    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(stream, wb);
    DB::writeIntBinary(shards, wb);
    DB::writeIntBinary(replicas, wb);
    DB::writeIntBinary(static_cast<uint8_t>(compacted), wb);
    DB::writeIntBinary(static_cast<uint8_t>(codec), wb);
    DB::writeIntBinary(flush_ms, wb);
    DB::writeIntBinary(flush_messages, wb);
    DB::writeVarInt(retention_ms, wb);
    DB::writeVarInt(retention_bytes, wb);
    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);

    wb.finalize();

    return data;
}

StreamDescription StreamDescription::deserialize(const char * data, size_t size)
{
    DB::ReadBufferFromMemory rb{data, size};

    StreamDescription desc;

    DB::readIntBinary(desc.version, rb);

    /// FIXME, dispatch according to version

    DB::readBinary(desc.id, rb);
    DB::readStringBinary(desc.ns, rb);
    DB::readStringBinary(desc.stream, rb);
    DB::readIntBinary(desc.shards, rb);
    DB::readIntBinary(desc.replicas, rb);

    uint8_t compacted = 0;
    DB::readIntBinary(compacted, rb);
    desc.compacted = compacted == 1;

    uint8_t codec = 0;
    DB::readIntBinary(codec, rb);
    desc.codec = static_cast<DB::CompressionMethodByte>(codec);

    DB::readIntBinary(desc.flush_ms, rb);
    DB::readIntBinary(desc.flush_messages, rb);
    DB::readVarInt(desc.retention_ms, rb);
    DB::readVarInt(desc.retention_bytes, rb);
    DB::readVarInt(desc.create_timestamp_ms, rb);
    DB::readVarInt(desc.last_modify_timestamp_ms, rb);

    return desc;
}

StreamDescription StreamDescription::from(std::string ns_, const CreateStreamRequest & req)
{
    StreamDescription desc;

    desc.version = 0;
    if (req.stream_id != DB::UUIDHelpers::Nil)
        desc.id = req.stream_id;
    else
        desc.id = DB::UUIDHelpers::generateV4();

    desc.ns = std::move(ns_);
    desc.stream = req.stream;
    desc.shards = req.shards;
    desc.replicas = req.replicas;
    desc.compacted = req.compacted;
    desc.codec = req.codec;
    desc.flush_ms = req.flush_ms;
    desc.flush_messages = req.flush_messages;
    desc.retention_ms = req.retention_ms;
    desc.retention_bytes = req.retention_bytes;
    desc.create_timestamp_ms = DB::UTCMilliseconds::now();
    desc.last_modify_timestamp_ms = desc.create_timestamp_ms;

    return desc;
}

std::string StreamDescription::string() const
{
    return fmt::format(
        "version={} id={} namespace={} stream={} shards={} replicas={} compacted={} codec={} flush_ms={} flush_messages={} retention_ms={} "
        "retention_bytes={} create_timestamp={} last_modify_timestamp={}",
        version,
        DB::toString(id),
        ns,
        stream,
        shards,
        replicas,
        compacted,
        static_cast<uint8_t>(codec),
        flush_ms,
        flush_messages,
        retention_ms,
        retention_bytes,
        create_timestamp_ms,
        last_modify_timestamp_ms);
}
}
