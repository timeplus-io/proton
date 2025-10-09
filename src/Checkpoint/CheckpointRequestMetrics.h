#pragma once

#include <Common/Stopwatch.h>

namespace DB
{
struct CheckpointRequestMetrics
{
    Stopwatch stopwatch;
    std::atomic<size_t> total_bytes = 0;
};
using CheckpointRequestMetricsPtr = std::shared_ptr<CheckpointRequestMetrics>;
}
