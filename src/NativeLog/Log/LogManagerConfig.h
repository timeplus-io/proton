#pragma once

#include <limits>

namespace nlog
{
struct LogManagerConfig
{
    uint32_t recovery_threads_per_data_dir = 1;
    uint32_t flush_check_ms = std::numeric_limits<int64_t>::max();
    uint32_t flush_recovery_offset_checkpoint_ms = 60000;
    uint32_t flush_start_offset_checkpoint_ms = 60000;
    uint32_t retention_check_ms = 5 * 60 * 1000;
    uint32_t max_pid_expiration_ms = 7 * 24 * 3600 * 1000;
};
}
