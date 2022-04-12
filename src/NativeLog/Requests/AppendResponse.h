#pragma once

#include "CommonResponse.h"

#include <NativeLog/Common/StreamShard.h>

namespace nlog
{
struct AppendResponse final : public CommonResponse
{
    AppendResponse(const StreamShard & stream_shard_, int32_t api_version_) : CommonResponse(api_version_), stream_shard(stream_shard_) { }

    StreamShard stream_shard;

    int64_t sn;
    int64_t log_start_sn;
    int64_t max_timestamp;
    int64_t append_timestamp;
};
}
