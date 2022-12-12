#pragma once
#include <cstdint>

namespace nlog
{
struct CommonRequest
{
    explicit CommonRequest(int16_t api_version_) : api_version(api_version_) { }

    int16_t api_version;
};
}
