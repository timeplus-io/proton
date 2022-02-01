#pragma once

#include "CommonResponse.h"

/// FIXME
#include <NativeLog/Common/FetchDataInfo.h>

namespace nlog
{
struct FetchResponse
{
    struct FetchData
    {
        std::string topic;
        int32_t partition;
        FetchDataInfo data;
    };

    std::vector<FetchData> data;

    CommonResponse error;
};
}
