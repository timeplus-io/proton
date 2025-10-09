#pragma once

#include <Cluster/Base/Concurrent/BlockingQueue.h>
#include <Cluster/Common/TimeWheel/TimeWheel.h>

#include <Common/CurrentMetrics.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

namespace cluster
{
class SystemTimer
{
public:
    /// \param worker_pool_size thread pool size which is used to execute timers' callbacks
    explicit SystemTimer(
        size_t worker_pool_size = 4,
        int64_t tick_ms = 1,
        uint64_t wheel_size = 6000,
        size_t expired_timer_size = 0,
        int64_t start_ms = DB::MonotonicMilliseconds::now());

    ~SystemTimer() { wait(); }

    /// It is caller's responsibility to make sure task's life cycle and its dependencies outlive SystemTimer.
    /// before it is removed
    /// Otherwise, undefined behavior may happen
    /// Caution: Repeating tasks require manual cancellation.
    [[nodiscard]] TimerTaskEntryPtr add(int64_t timeout_ms, TimerCallback task, bool repeat = false);

    /// \return number of {num_of_timers_reinserted, number_of_timers_expired}
    std::pair<size_t, size_t> poll(int64_t timeout_ms);

    uint64_t size() const noexcept { return num_timers; }

    void wait();

private:
    /// \return true if added otherwise false
    bool addTimerTaskEntryLocked(TimerTaskEntryPtr timer_task_entry);

    void executeExpiredTimers();

private:
    std::atomic_flag stopped;

    std::atomic_uint64_t num_timers = 0;

    BlockingQueue<TimerTaskEntryPtr> expired_timers;

    /// \worker_pool is used to execute timers' callbacks
    ThreadPool worker_pool;

    /// The delay_queue works across the SystemTimer (consumer) and TimeWheel (producer).
    /// Thus, it needs to be shared.
    /// The delay_queue internally holds Buckets.
    /// Shared semantics are used so that the wheel bucket and the queue bucket reference the same bucket.
    /// In some cases: the bucket insert new task, but expiration time is not change thus no new enqueue behaviour happens.
    std::shared_ptr<DelayQueue> delay_queue;

    mutable DB::SharedMutex wheel_rw_mutex;
    TimeWheel time_wheel;
    LoggerPtr logger;
};
}
