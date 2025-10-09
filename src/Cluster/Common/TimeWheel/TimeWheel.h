#pragma once

#include <Cluster/Common/TimeWheel/DelayQueue.h>

namespace cluster
{

class TimeWheel
{
public:
    TimeWheel(
        int64_t tick_ms_,
        uint64_t wheel_size_,
        int64_t start_ms_,
        std::shared_ptr<DelayQueue> queue_,
        std::function<void()> decrement_size_ = {});

    ~TimeWheel() = default;

    bool add(TimerTaskEntryPtr timer_task_entry);
    void advanceClock(int64_t time_ms);

private:
    void addOverflowWheel();

private:
    int64_t tick_ms;
    uint64_t wheel_size;
    int64_t interval_ms;
    std::atomic<int64_t> current_time_ms;

    std::shared_ptr<DelayQueue> queue;
    std::function<void()> decrement_size;

    std::vector<TimerTaskListPtr> buckets;
    std::unique_ptr<TimeWheel> overflow_wheel;
};

}
