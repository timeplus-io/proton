#pragma once

#include <memory>
#include <string>

#include <fmt/format.h>

namespace cluster::nlog
{
struct FetchConfig
{
    int64_t max_wait_ms = 500;
    uint64_t max_bytes = 8 * 1024 * 1024;

    void validate()
    {
        if (max_wait_ms <= 0)
            max_wait_ms = 500;

        if (max_bytes == 0)
            max_bytes = 8 * 1024 * 1024;
    }

    std::string string() const { return fmt::format("max_wait_ms={} max_bytes={}", max_wait_ms, max_bytes); }
};

using FetchConfigPtr = std::shared_ptr<FetchConfig>;
}
