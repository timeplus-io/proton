#pragma once

#include <atomic>

namespace DB
{
struct StreamIOStats
{
    std::atomic<int64_t> metric_time;
    std::atomic<uint64_t> read_bytes;
    std::atomic<uint64_t> read_rows;
    std::atomic<uint64_t> written_bytes;
    std::atomic<uint64_t> written_rows;
    std::atomic<uint64_t> public_read_bytes;
    std::atomic<uint64_t> public_read_rows;
    std::atomic<uint64_t> public_written_bytes;
    std::atomic<uint64_t> public_written_rows;

    explicit StreamIOStats(int64_t metric_time_) : metric_time(metric_time_) { }
};
}
