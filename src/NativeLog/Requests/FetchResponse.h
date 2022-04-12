#pragma once

#include "CommonResponse.h"

#include <NativeLog/Common/FetchDataDescription.h>
#include <NativeLog/Common/StreamShard.h>

namespace nlog
{
struct FetchResponse final : public CommonResponse
{
public:
    struct FetchedData
    {
        StreamShard stream_shard;
        int32_t errcode;
        FetchDataDescription data;
    };

public:
    explicit FetchResponse(std::vector<FetchedData> fetched_data_, int32_t api_version_ = 0)
        : CommonResponse(api_version_), fetched_data(std::move(fetched_data_))
    {
    }

    std::vector<FetchedData> fetched_data;
};
}
