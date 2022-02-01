#pragma once

namespace nlog
{
struct TruncateRequest
{
    std::string topic;
    int32_t partition;
    int64_t offset;
};
}
