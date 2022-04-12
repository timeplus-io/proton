#pragma once

#include "CommonResponse.h"

namespace nlog
{
struct TranslateTimestampsResponse : public CommonResponse
{
    TranslateTimestampsResponse(Stream stream_, int32_t api_version_ = 0) : CommonResponse(api_version_), stream(std::move(stream_)) { }

    Stream stream;
    std::vector<int64_t> sequences;
};
}
