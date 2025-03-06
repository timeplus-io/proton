#include <Common/TimeBasedThrottler.h>

namespace DB
{

TimeBasedThrottler::TimeBasedThrottler(UInt64 suspend_period_ms_) : suspend_period_ms(suspend_period_ms_)
{
}

void TimeBasedThrottler::execute(std::function<void(UInt64 /*count*/)> fn)
{
    ++count;

    if (timer.elapsedMilliseconds() >= suspend_period_ms)
    {
        fn(count);
        timer.restart();
    }
}

void TimeBasedThrottler::reset()
{
    count = 0;
    timer.restart();
}

}
