#pragma once

#include <base/ClockUtils.h>
#include <base/defines.h>

#include <functional>
#include <list>
#include <memory>
#include <mutex>


namespace cluster
{

class TimerTaskList;
using TimerTaskListPtr = std::shared_ptr<TimerTaskList>;
using TimerCallback = std::function<void()>;

class TimerTaskEntry : public std::enable_shared_from_this<TimerTaskEntry>
{
public:
    TimerTaskEntry(int32_t interval_ms_, TimerCallback task_, bool is_repeat_ = false);
    ~TimerTaskEntry() = default;

    void cancel();
    bool isCancelled() const noexcept;
    bool isRepeat() const noexcept { return is_repeat; }
    void execute() const { task(); }
    int64_t expiration() const noexcept { return expiration_ms; }
    void restart() noexcept { expiration_ms = DB::MonotonicMilliseconds::now() + interval_ms; }

private:
    using ListIterator = std::list<std::shared_ptr<TimerTaskEntry>>::iterator;

public:
    std::mutex & getMutex() noexcept { return mutex; }

    /// Assumes both entry mutex and bucket mutex are held
    void updateOwnerListAndPositionLocked(TimerTaskListPtr new_owner, std::optional<ListIterator> new_position);

    void clearOwnerListAndPosition();
    void clearOwnerListAndPositionLocked();

    /// After detachFromCurrentBucket(), the task is either:
    /// a) Cancelled (via cancel())
    /// b) Being moved to a new bucket (via TimeWheel::add)
    /// Always lock in order: entry mutex -> bucket mutex
    void detachFromCurrentBucket();

private:
    mutable std::mutex mutex;
    std::atomic<bool> cancelled{false};
    std::optional<ListIterator> position;
    std::weak_ptr<TimerTaskList> owner;

    int64_t expiration_ms;
    TimerCallback task;
    int32_t interval_ms;
    bool is_repeat;
};

using TimerTaskEntryPtr = std::shared_ptr<TimerTaskEntry>;
}
