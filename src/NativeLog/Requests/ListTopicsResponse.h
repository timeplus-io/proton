#pragma once

#include "CommonResponse.h"

namespace nlog
{
struct ListTopicsResponse
{
    std::vector<TopicInfo> topics;

    CommonResponse error;
};
}
