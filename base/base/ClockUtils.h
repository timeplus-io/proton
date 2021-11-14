#pragma once

#include "types.h"

#include <chrono>

namespace DB
{
template<typename Clock, typename TimeScale>
class ClockUtils
{
public:
    static inline Int64 now()
    {
        return std::chrono::duration_cast<TimeScale>(Clock::now().time_since_epoch()).count();
    }
};

template<typename TimeScale>
using UTCClock = ClockUtils<std::chrono::system_clock, TimeScale>;

template<typename TimeScale>
using MonotonicClock = ClockUtils<std::chrono::steady_clock, TimeScale>;

using UTCSeconds = UTCClock<std::chrono::seconds>;
using UTCMilliseconds = UTCClock<std::chrono::milliseconds>;
using UTCMicroseconds = UTCClock<std::chrono::microseconds>;
using UTCNanoseconds = UTCClock<std::chrono::nanoseconds>;

using MonotonicSeconds = MonotonicClock<std::chrono::seconds>;
using MonotonicMilliseconds = MonotonicClock<std::chrono::milliseconds>;
using MonotonicMicroseconds = MonotonicClock<std::chrono::microseconds>;
using MonotonicNanoseconds = MonotonicClock<std::chrono::nanoseconds>;

}
