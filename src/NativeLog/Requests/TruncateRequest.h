#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/Stream.h>

#include <string>

namespace nlog
{
struct TruncateRequest final : public CommonRequest
{
public:
    TruncateRequest(std::string stream_, const StreamID & stream_id_, int32_t shard_, int64_t truncated_to_sn_, int16_t api_version_ = 0)
        : CommonRequest(api_version_), stream(std::move(stream_)), stream_id(stream_id_), shard(shard_), sn(truncated_to_sn_)
    {
    }

    std::string stream;
    StreamID stream_id;
    int32_t shard;

    /// sequence truncated to
    int64_t sn;
};
}
