#pragma once

#include <queue>
#include <shared_mutex>
#include <condition_variable>

namespace nlog
{
/// A naive blocking queue implementation.
/// Revisit this

template <typename T>
class BlockingQueue
{
public:
    explicit BlockingQueue(size_t max_size_) : max_size(max_size_) { }

    /// add will be blocked if queue is full
    void add(const T & v)
    {
        {
            std::unique_lock guard{qlock};
            cv.wait(guard, [this] { return queue.size() < max_size; });

            queue.push(v);
        }

        /// Notify front() we have value
        cv.notify_one();
    }

    void add(T && v)
    {
        {
            std::unique_lock guard{qlock};
            cv.wait(guard, [this] { return queue.size() < max_size; });

            queue.push(std::move(v));
        }

        /// Notify front() we have value
        cv.notify_one();
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        {
            std::unique_lock guard{qlock};

            cv.wait(guard, [this] { return queue.size() < max_size; });

            queue.emplace(std::forward<Args>(args)...);
        }

        /// Notify front() we have value
        cv.notify_one();
    }

    /// get and pop front
    T take()
    {
        std::unique_lock guard{qlock};
        cv.wait(guard, [this] { return !queue.empty(); });

        T t = queue.front();
        queue.pop();

        /// Manually unlocking is done before notifying to avoid waking up
        /// the waiting thread only to block again
        guard.unlock();

        /// Notify push/emplace, there is empty slot
        cv.notify_one();

        return t;
    }

    /// Get front. If queue is empty, wait forever for one
    T peek() const
    {
        std::shared_lock guard{qlock};
        cv.wait(guard, [this] { return !queue.empty(); });
        return queue.front();
    }

    size_t size() const
    {
        std::shared_lock guard{qlock};
        return queue.size();
    }

    bool empty() const
    {
        std::shared_lock guard{qlock};
        return queue.empty();
    }

private:
    const size_t max_size;

    std::condition_variable_any cv;
    mutable std::shared_mutex qlock;
    std::queue<T> queue;
};
}
