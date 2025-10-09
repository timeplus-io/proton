#pragma once

#include <Cluster/Common/TimeWheel/SystemTimer.h>

#include <Common/CurrentMetrics.h>


namespace cluster
{

class TimerService
{
public:
    explicit TimerService(size_t worker_pool_size = 4, int64_t tick_ms = 100, uint64_t wheel_size = 6000, size_t expired_timers_size = 0);

    ~TimerService();

    void shutdown();

    [[nodiscard]] TimerTaskEntryPtr add(int64_t timeout_ms, TimerCallback task, bool repeat = false);

private:
    void loop();

private:
    std::atomic_flag stopped;
    SystemTimer timer;
    ThreadPool poller;
    LoggerPtr logger;
};
}
