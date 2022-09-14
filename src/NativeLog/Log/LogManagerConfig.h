#pragma once

#include <limits>
#include <string>

#include <fmt/format.h>

namespace nlog
{
struct LogManagerConfig
{
    uint32_t recovery_threads_per_data_dir = 1;
    uint32_t flush_check_ms = std::numeric_limits<uint32_t>::max();
    uint32_t flush_recovery_sn_checkpoint_ms = 60000;
    uint32_t flush_start_sn_checkpoint_ms = 60000;
    uint32_t retention_check_ms = 5 * 60 * 1000;
    uint32_t max_pid_expiration_ms = 7 * 24 * 3600 * 1000;

    std::string string() const
    {
        return fmt::format(
            "recovery_threads_per_data_dir={} flush_check_ms={} flush_recovery_sn_checkpoint_ms={} flush_start_sn_checkpoint_ms={} "
            "retention_check_ms={}",
            recovery_threads_per_data_dir,
            flush_check_ms,
            flush_recovery_sn_checkpoint_ms,
            flush_start_sn_checkpoint_ms,
            retention_check_ms);
    }
};
}
