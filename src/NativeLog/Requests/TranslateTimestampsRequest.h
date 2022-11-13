#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/Stream.h>

namespace nlog
{
struct TranslateTimestampsRequest : public CommonRequest
{
    TranslateTimestampsRequest(Stream stream_, std::vector<int32_t> shards_, std::vector<int64_t> timestamps_, bool append_time_, int32_t api_version_ = 0)
        : CommonRequest(api_version_), stream(std::move(stream_)), shards((std::move(shards_))), timestamps(std::move(timestamps_)), append_time(append_time_)
    {
        assert(shards.size() == timestamps.size());
    }

    Stream stream;
    std::vector<int32_t> shards;
    std::vector<int64_t> timestamps;
    bool append_time;
};
}
