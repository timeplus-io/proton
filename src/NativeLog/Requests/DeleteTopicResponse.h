#pragma once

#include "CommonResponse.h"

#include <string>

namespace nlog
{
struct DeleteTopicResponse
{
    CommonResponse error;

    std::string string() const
    {
        return error.error_message;
    }
};
}
