#pragma once

#include <memory>
#include <string>

#include <fmt/format.h>

namespace nlog
{
struct FetchConfig
{
    int64_t max_wait_ms = 500;
    int64_t max_bytes = 8 * 1024 * 1024;

    std::string string() const { return fmt::format("max_wait_ms={} max_bytes={}", max_wait_ms, max_bytes); }
};
}
