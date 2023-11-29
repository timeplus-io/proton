#pragma once

#include <Common/ThreadPool.h>

#include <muduo/net/EventLoop.h>

namespace DB
{
/// TimerService runs callbacks at specific time at best effort in a separated thread.
/// Callbacks are expected to run quick to avoid stall the whole timer pipeline
class TimerService final
{
public:
    void startup();
    void shutdown();

    /// Runs callback at 'time'.
    /// Safe to call from other threads.
    muduo::net::TimerId runAt(muduo::Timestamp time, muduo::net::TimerCallback cb)
    {
        assert(event_loop);
        return event_loop->runAt(time, std::move(cb));
    }

    /// Runs callback after @c delay seconds.
    /// Safe to call from other threads.
    muduo::net::TimerId runAfter(double delay, muduo::net::TimerCallback cb)
    {
        assert(event_loop);
        return event_loop->runAfter(delay, std::move(cb));
    }

    /// Runs callback every @c interval seconds.
    /// Safe to call from other threads.
    muduo::net::TimerId runEvery(double interval, muduo::net::TimerCallback cb)
    {
        assert(event_loop);
        return event_loop->runEvery(interval, std::move(cb));
    }

    /// Cancels the timer.
    /// Safe to call from other threads.
    void cancel(muduo::net::TimerId timer_id)
    {
        assert(event_loop);
        return event_loop->cancel(timer_id);
    }

private:
    void startEventLoop();

private:
    ThreadPool looper;
    std::shared_ptr<muduo::net::EventLoop> event_loop;
    std::atomic<muduo::net::EventLoop *> eloop_init_guard = nullptr;
};
}
