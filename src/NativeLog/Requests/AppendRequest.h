#pragma once

#include "CommonRequest.h"

#include <NativeLog/Record/Record.h>
#include <NativeLog/Common/StreamShard.h>

namespace nlog
{
/// Append a record to the target shard of a stream
struct AppendRequest final : public CommonRequest
{
    AppendRequest(std::string stream_, const StreamID & stream_id_, int32_t shard_, RecordPtr record_, int16_t api_version_ = 0)
        : CommonRequest(api_version_), stream_shard(std::move(stream_), stream_id_, shard_), record(std::move(record_))
    {
    }

    StreamShard stream_shard;
    RecordPtr record;
};
}
