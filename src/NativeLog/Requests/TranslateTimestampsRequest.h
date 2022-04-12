#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/Stream.h>

namespace nlog
{
struct TranslateTimestampsRequest : public CommonRequest
{
    TranslateTimestampsRequest(Stream stream_, std::vector<int32_t> shards_, int64_t timestamp_, bool append_time_, int32_t api_version_ = 0)
        : CommonRequest(api_version_), stream(std::move(stream_)), shards((std::move(shards_))), timestamp(timestamp_), append_time(append_time_)
    {
    }

    Stream stream;
    std::vector<int32_t> shards;
    int64_t timestamp;
    bool append_time;
};
}
