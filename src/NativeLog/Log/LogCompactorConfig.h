#pragma once

#include <cstdint>
#include <limits>
#include <string>

#include <fmt/format.h>

namespace nlog
{
struct LogCompactorConfig
{
    uint32_t num_threads = 1;
    uint64_t dedup_buffer_size = 4 * 1024 * 1024;
    double dedup_load_factor = 0.9;
    uint32_t io_buffer_size = 1024 * 1024;
    double max_io_bytes_per_second = std::numeric_limits<double>::max();
    uint32_t backoff_ms = 15 * 1000;
    bool enable_compactor = true;
    std::string hash_algo = "MD5";

    std::string string() const
    {
        return fmt::format(
            "num_threads={} dedup_buffer_size={} dedup_load_factor={} io_buffer_size={} max_io_bytes_per_second={} backoff_ms={} "
            "enable_compactor={} hash_algo={}",
            num_threads,
            dedup_buffer_size,
            dedup_load_factor,
            io_buffer_size,
            max_io_bytes_per_second,
            backoff_ms,
            enable_compactor,
            hash_algo);
    }
};
}
