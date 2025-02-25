#pragma once

#include <base/types.h>

#include <queue>
#include <shared_mutex>
#include <condition_variable>
#include <cassert>

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

    bool add(T && v, int64_t timeout_ms)
    {
        {
            std::unique_lock lock{qlock};

            if (queue.size() >= max_size)
            {
                auto status = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return queue.size() < max_size; });
                if (!status)
                    return false;
            }

            queue.push(std::move(v));
            assert(queue.size() <= max_size);
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();

        return true;
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

    /// get and pop front if not timeout
    /// return empty if timeout
    std::optional<T> take(const UInt64 timeout_ms)
    {
        std::unique_lock guard{qlock};
        auto status = cv.wait_for(guard, std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); });

        if (!status)
            return {};

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

    std::queue<T> drain()
    {
        std::queue<T> r;
        {
            std::unique_lock lock{qlock};
            /// When queue is empty, we don't want to steal its
            /// underlying allocated memory.
            if (queue.empty())
                return {};

            r.swap(queue);
            assert(queue.empty());
        }
        cv.notify_all();

        return r;
    }

    std::queue<T> drain(int64_t timeout_ms)
    {
        std::queue<T> r;
        {
            std::unique_lock lock{qlock};

            if (queue.empty())
            {
                auto status = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); });
                if (!status)
                    return {};
            }

            r.swap(queue);
            assert(queue.empty());
        }
        cv.notify_all();

        return r;
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
