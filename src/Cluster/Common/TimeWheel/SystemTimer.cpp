#include <Cluster/Common/TimeWheel/SystemTimer.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace cluster
{
SystemTimer::SystemTimer(size_t worker_pool_size, int64_t tick_ms, uint64_t wheel_size, size_t expired_timer_size, int64_t start_ms)
    : expired_timers(expired_timer_size ? expired_timer_size : std::max<size_t>(worker_pool_size * 128, 512))
    , worker_pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, worker_pool_size)
    , delay_queue(std::make_shared<DelayQueue>())
    , time_wheel(tick_ms, wheel_size, start_ms, delay_queue, /*decrement_size_=*/[this]() { --num_timers; })
    , logger(getLogger("TimerSchedPool"))
{
    for (size_t i = 0; i < worker_pool_size; ++i)
        worker_pool.scheduleOrThrowOnError([this]() {
            setThreadName("SysTimeWorker");
            executeExpiredTimers();
        });
}

[[nodiscard]] TimerTaskEntryPtr SystemTimer::add(int64_t timeout_ms, TimerCallback task, bool repeat)
{
    auto task_entry = std::make_shared<TimerTaskEntry>(timeout_ms, std::move(task), repeat);

    std::shared_lock lock(wheel_rw_mutex);

    if (addTimerTaskEntryLocked(task_entry))
    {
        ++num_timers;
        return task_entry;
    }
    else
    {
        return nullptr;
    }
}

std::pair<size_t, size_t> SystemTimer::poll(int64_t timeout_ms)
{
    size_t timers_reinserted = 0;
    size_t timers_expired = 0;

    if (auto bucket_opt = delay_queue->poll(timeout_ms); bucket_opt)
    {
        std::unique_lock wheel_lock(wheel_rw_mutex);

        do
        {
            auto & bucket = bucket_opt.value();

            chassert(bucket->hasExpiration());
            time_wheel.advanceClock(bucket->getExpiration());
            bucket->flush([this, &timers_reinserted, &timers_expired](TimerTaskEntryPtr timer_task_entry) {
                auto added = this->addTimerTaskEntryLocked(std::move(timer_task_entry));
                if (added)
                {
                    timers_reinserted++;
                }
                else
                {
                    --num_timers;
                    timers_expired++;
                }
            });

            bucket_opt = delay_queue->poll();
        } while (bucket_opt);
    }
    return {timers_reinserted, timers_expired};
}

void SystemTimer::wait()
{
    if (stopped.test_and_set())
        return;

    worker_pool.wait();
}

bool SystemTimer::addTimerTaskEntryLocked(TimerTaskEntryPtr timer_task_entry)
{
    chassert(timer_task_entry);
    /// Remove this assertion as it's too strict for hierarchical wheel
    /// because the task may be cancelled by higher layers
    /// chassert(!timer_task_entry->isCancelled());

    if (!time_wheel.add(timer_task_entry))
    {
        /// Already expired or cancelled
        if (!timer_task_entry->isCancelled())
            expired_timers.add(std::move(timer_task_entry));
        return false;
    }
    return true;
}

void SystemTimer::executeExpiredTimers()
{
    while (!stopped.test(std::memory_order_relaxed))
    {
        if (auto optional_timer_task = expired_timers.take(500); optional_timer_task)
        {
            if (auto timer_task = std::move(optional_timer_task.value()); timer_task && !timer_task->isCancelled())
            {
                try
                {
                    timer_task->execute();
                }
                catch (const DB::Exception & e)
                {
                    LOG_ERROR(logger, "Internal error in timer task: error={}.", e.message());
                }
                catch (const std::exception & e)
                {
                    LOG_ERROR(logger, "Internal error executing timer task: error={}.", e.what());
                }
                catch (...)
                {
                    DB::tryLogCurrentException(logger, "Internal error executing scheduled timer task");
                }

                if (timer_task->isRepeat() && !timer_task->isCancelled())
                {
                    /// For repeat timer, once it expired, re-arm it in SystemTimer after callback is invoked unless cancelled, regardless of whether an exception occurred
                    try
                    {
                        timer_task->restart();
                        num_timers += addTimerTaskEntryLocked(std::move(timer_task));
                    }
                    catch (...)
                    {
                        DB::tryLogCurrentException(logger, "Caught exception while re-arming scheduled timer task");
                    }
                }
                else
                {
                    timer_task.reset();
                }
            }
            else
            {
                LOG_ERROR(logger, "Internal error: Invalid timer task encountered.");
            }
        }
    }
}
}
