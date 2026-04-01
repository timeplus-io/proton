#include <Cluster/Common/TimeWheel/TimeWheel.h>


namespace cluster
{

TimeWheel::TimeWheel(
    int64_t tick_ms_, uint64_t wheel_size_, int64_t start_ms_, std::shared_ptr<DelayQueue> queue_, std::function<void()> decrement_size_)
    : tick_ms(tick_ms_)
    , wheel_size(wheel_size_)
    , interval_ms(tick_ms_ * wheel_size_)
    , queue(queue_)
    , decrement_size(std::move(decrement_size_))
    , buckets(wheel_size_)
{
    chassert(tick_ms_ > 0);
    chassert(start_ms_ > 0);
    current_time_ms = start_ms_ - (start_ms_ % tick_ms_);

    for (auto & bucket : buckets)
        bucket = std::make_shared<TimerTaskList>(decrement_size);
}

bool TimeWheel::add(TimerTaskEntryPtr timer_task_entry)
{
    if (!timer_task_entry || timer_task_entry->isCancelled())
        return false;

    int64_t expiration = timer_task_entry->expiration();
    int64_t current_time = current_time_ms.load();
    if (expiration < current_time + tick_ms)
    {
        /// Already expired
        return false;
    }
    else if (expiration < current_time + interval_ms)
    {
        /// Expiration falls into the current wheel, put it in its own wheel bucket
        int64_t virtual_id = expiration / tick_ms;
        uint64_t bucket_id = virtual_id % wheel_size;

        TimerTaskListPtr & bucket = buckets[bucket_id];

        timer_task_entry->detachFromCurrentBucket();

        /// Add to new bucket - we know task is valid here
        bucket->add(std::move(timer_task_entry));

        /// Set the bucket expiration time
        if (bucket->setExpiration(virtual_id * tick_ms))
        {
            /// The bucket needs to be enqueued because it was an expired bucket
            /// We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
            /// and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
            /// will pass in the same value and hence return false, thus the bucket with the same expiration will not
            /// be enqueued multiple times.
            queue->offer(bucket);
        }

        return true;
    }
    else
    {
        /// Out of the interval. Put it into the parent timer
        auto * overflow = getOrCreateOverflowWheel();
        chassert(overflow);
        return overflow->add(std::move(timer_task_entry));
    }
}

void TimeWheel::advanceClock(int64_t time_ms)
{
    chassert(time_ms > 0);

    int64_t current_time = current_time_ms.load();
    if (time_ms >= current_time + tick_ms)
    {
        int64_t new_time = time_ms - (time_ms % tick_ms);
        current_time_ms.store(new_time);

        /// Benign race: overflow_wheel is monotonic (nullptr → valid, never back).                                                                                           
        /// A racy nullptr read just skips one tick; the next call catches up.                                                                                                
        /// We still call getOverflowWheel() (mutex-guarded) to obtain a safe pointer.                                                                                        
        if (overflow_wheel)                                                                                                                                            
        {                                                                                                                                                              
            auto * overflow = getOverflowWheel();                                                                                                                      
            overflow->advanceClock(new_time);
        }
    }
}

TimeWheel * TimeWheel::getOverflowWheel()
{
    std::lock_guard lock(overflow_wheel_mutex);
    return overflow_wheel.get();
}

TimeWheel * TimeWheel::getOrCreateOverflowWheel()
{
    std::lock_guard lock(overflow_wheel_mutex);
    if (!overflow_wheel)
        overflow_wheel = std::make_unique<TimeWheel>(interval_ms, wheel_size, current_time_ms.load(), queue, decrement_size);

    return overflow_wheel.get();
}

}
