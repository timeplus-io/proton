#pragma once

#include <Common/Stopwatch.h>

namespace DB
{

/// A throttler that restricts how often an operation can perform.
class TimeBasedThrottler final
{
public:
    /// \suspend_period_ms_ - how long it should wait before an operation can be executed again.
    explicit TimeBasedThrottler(UInt64 suspend_period_ms_);

    /// Execute an operation if it's not throttled.
    /// If the operation is throttled, it returns immediately without doing anything.
    void execute(std::function<void(UInt64 /*count*/)> fn);

    /// Reset the throttler to the initial state.
    void reset();

private:
    UInt64 suspend_period_ms{0};
    UInt64 count{0};
    Stopwatch timer;
};

}
