#pragma once

#include <Cluster/Common/TimeWheel/TimerTask.h>

#include <atomic>


namespace cluster
{

class TimerTaskList : public std::enable_shared_from_this<TimerTaskList>
{
public:
    explicit TimerTaskList(std::function<void()> decrement_size_ = {});
    ~TimerTaskList() = default;

    void add(TimerTaskEntryPtr timer_task);
    void remove(std::list<TimerTaskEntryPtr>::iterator pos);
    void flush(std::function<void(TimerTaskEntryPtr)> func);

    bool setExpiration(int64_t expiration_time) noexcept;
    int64_t getExpiration() const noexcept;
    bool hasExpiration() const noexcept;
    std::chrono::milliseconds getDelay() const noexcept;

    std::mutex & getBucketMutex() noexcept { return bucket_mutex; }

    void removeLocked(std::list<TimerTaskEntryPtr>::iterator pos)
    {
        bucket.erase(pos);

        if (decrement_size)
            decrement_size();
    }

private:
    std::function<void()> decrement_size;

    std::atomic_int64_t expiration_ms; /// abs time
    mutable std::mutex bucket_mutex;
    std::list<TimerTaskEntryPtr> bucket;
};
}
