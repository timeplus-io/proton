#include <Cluster/Common/TimeWheel/TimerTaskList.h>

namespace cluster
{

TimerTaskList::TimerTaskList(std::function<void()> decrement_size_) : decrement_size(std::move(decrement_size_)), expiration_ms(-1)
{
}

/// Since caller (TimeWheel) guarantees:
/// - task is not null
/// - task is not cancelled
/// - task needs to be moved to this bucket
void TimerTaskList::add(TimerTaskEntryPtr entry)
{
    /// Pre-check without locks to fail fast
    chassert(entry);

    /// Lock order: {bucket_mutex, entry_mutex}
    std::scoped_lock locks(bucket_mutex, entry->getMutex());

    bucket.push_back(entry);
    auto pos = std::prev(bucket.end());
    entry->updateOwnerListAndPositionLocked(shared_from_this(), pos);
}

void TimerTaskList::remove(std::list<TimerTaskEntryPtr>::iterator pos)
{
    std::scoped_lock bucket_guard(bucket_mutex);
    removeLocked(pos);
}

void TimerTaskList::flush(std::function<void(TimerTaskEntryPtr)> reinsert)
{
    std::list<TimerTaskEntryPtr> active_tasks;
    {
        std::scoped_lock bucket_guard(bucket_mutex);
        active_tasks.splice(active_tasks.end(), bucket);

        /// We need to hold on to the bucket_mutex when clear owner list to make sure
        /// the task clear the current owner instead of other owner (owner changed)
        for (const auto & task : active_tasks)
            task->clearOwnerListAndPosition();
    }

    for (auto & task : active_tasks)
        reinsert(std::move(task));

    expiration_ms = -1;
}

/// https://github.com/apache/kafka/blob/e3f953483cb480631bf041698770b47ddb82796f/server-common/src/main/java/org/apache/kafka/server/util/timer/TimerTaskList.java#L55-L57
/// Return `true` when the expiration time changes and `false` when it remains the same. https://godbolt.org/z/fqfYeenzv
bool TimerTaskList::setExpiration(int64_t expiration_time) noexcept
{
    return expiration_ms.exchange(expiration_time) != expiration_time;
}

int64_t TimerTaskList::getExpiration() const noexcept
{
    return expiration_ms;
}

bool TimerTaskList::hasExpiration() const noexcept
{
    return getExpiration() != -1;
}

std::chrono::milliseconds TimerTaskList::getDelay() const noexcept
{
    auto now = DB::MonotonicMilliseconds::now();
    auto nearest = getExpiration();

    return std::chrono::milliseconds(std::max((nearest - now), static_cast<int64_t>(0)));
}

}
