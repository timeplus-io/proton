#pragma once

#include "CommonResponse.h"
#include "TopicInfo.h"

#include <vector>

namespace nlog
{
struct ListTopicsResponse
{
    std::vector<TopicInfo> topics;

    CommonResponse error;
};
}
