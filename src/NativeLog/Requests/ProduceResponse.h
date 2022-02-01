#pragma once

#include "CommonResponse.h"

namespace nlog
{
struct ProduceResponse
{
    int64_t first_offset;
    int64_t last_offset;
    int64_t max_timestamp;
    int64_t append_timestamp;
    int64_t log_start_offset;

    CommonResponse error;
};
}
