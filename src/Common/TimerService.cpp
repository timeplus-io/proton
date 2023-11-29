#include <Common/TimerService.h>

namespace DB
{
void TimerService::startup()
{
    looper.scheduleOrThrowOnError([this] { startEventLoop(); });

    /// Wait until the guard is not nullptr
    eloop_init_guard.wait(nullptr);
}

void TimerService::shutdown()
{
    event_loop->quit();

    /// Decrement ref count to make sure the last ref count
    /// of event loop is in the creation thread
    event_loop = nullptr;
    eloop_init_guard = nullptr;
    eloop_init_guard.notify_all();

    looper.wait();
}

void TimerService::startEventLoop()
{
    /// Event loop needs run and dtor in the its init thread
    auto eloop = std::make_shared<muduo::net::EventLoop>();
    event_loop = eloop;
    eloop_init_guard = eloop.get();
    eloop_init_guard.notify_all();

    event_loop->loop();

    /// Wait until guard is cleared by shutdown
    eloop_init_guard.wait(eloop.get());
}
}
