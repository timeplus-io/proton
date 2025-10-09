#include <Cluster/Common/TimeWheel/TimerTask.h>
#include <Cluster/Common/TimeWheel/TimerTaskList.h>

namespace cluster
{

TimerTaskEntry::TimerTaskEntry(int32_t interval_ms_, TimerCallback task_, bool is_repeat_)
    : expiration_ms(DB::MonotonicMilliseconds::now() + interval_ms_)
    , task(std::move(task_))
    , interval_ms(interval_ms_)
    , is_repeat(is_repeat_)
{
}

bool TimerTaskEntry::isCancelled() const noexcept
{
    return cancelled.load(std::memory_order_acquire);
}

void TimerTaskEntry::clearOwnerListAndPosition()
{
    std::scoped_lock lock{mutex};
    clearOwnerListAndPositionLocked();
}

void TimerTaskEntry::clearOwnerListAndPositionLocked()
{
    /// When we clear current owner, it shall have a owner
    chassert(!owner.expired() && position.has_value());

    owner.reset();
    position = std::nullopt;
}

void TimerTaskEntry::updateOwnerListAndPositionLocked(TimerTaskListPtr new_owner, std::optional<ListIterator> new_position)
{
    /// Entry shall not have an owner when it gets reset to a new owner
    /// since we make sure its owner is clear up before we reach here
    chassert(owner.expired());

    owner = new_owner;
    position = new_position;
}

void TimerTaskEntry::cancel()
{
    cancelled.store(true, std::memory_order_release);
    detachFromCurrentBucket();
}

void TimerTaskEntry::detachFromCurrentBucket()
{
    TimerTaskListPtr current_owner;
    std::optional<ListIterator> current_position;
    {
        std::scoped_lock task_lock(mutex);

        current_owner = owner.lock();
        current_position = position;
        if (!current_owner || !current_position)
            return;
    }

    chassert(current_owner && current_position);
    {
        /// Lock order: {bucket_mutex, entry_mutex}, shall be same as in TimerTaskList::add(...)
        std::scoped_lock locks(current_owner->getBucketMutex(), mutex);

        /// Guardrail : if owner gets changed or position gets changed, a re-insert already happens for this task entry,
        /// avoid removing from the current list
        if (auto owner_ptr = owner.lock(); owner_ptr == current_owner && (position.value() == current_position.value()))
        {
            current_owner->removeLocked(*position);
            clearOwnerListAndPositionLocked();
        }
    }
}

}
