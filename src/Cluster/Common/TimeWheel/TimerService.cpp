#include <Cluster/Common/TimeWheel/TimerService.h>

#include <Common/Stopwatch.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace cluster
{

TimerService::TimerService(size_t worker_pool_size, int64_t tick_ms, uint64_t wheel_size, size_t expired_timers_size)
    : timer(worker_pool_size, tick_ms, wheel_size, expired_timers_size)
    , poller{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1}
    , logger(getLogger("TimerService"))
{
    poller.scheduleOrThrowOnError([this]() {
        setThreadName("TimerService");
        loop();
    });
}

TimerService::~TimerService()
{
    shutdown();
}

void TimerService::shutdown()
{
    if (stopped.test_and_set())
        return;

    /// Best-effort shutdown: stop polling first so active repeat timers cannot keep the
    /// current poll cycle self-feeding and block shutdown indefinitely.
    poller.wait();
    timer.wait();

    /// TODO: if shutdown ever needs lossless timer draining, introduce an explicit
    /// producer/rearm quiesce protocol instead of relying on this order alone.
}

TimerTaskEntryPtr TimerService::add(int64_t timeout_ms, TimerCallback task, bool repeat)
{
    return timer.add(timeout_ms, std::move(task), repeat);
}

void TimerService::loop()
{
    constexpr size_t timer_poll_timeout_ms = 1000;
    constexpr size_t timer_processing_threshold = 2 * timer_poll_timeout_ms;

    Stopwatch stopwatch;
    while (!stopped.test(std::memory_order_relaxed))
    {
        stopwatch.restart();

        auto [reinserted, expired] = timer.poll(/*timeout_ms=*/timer_poll_timeout_ms);

        auto elapsed = stopwatch.elapsedMilliseconds();
        if (elapsed >= timer_processing_threshold)
            LOG_WARNING(logger, "Took {}ms to poll timers, reinserted={} expired={}. Timer seems slow", elapsed, reinserted, expired);
    }
}

}
