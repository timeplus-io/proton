#pragma once

#include <cassert>
#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>

namespace cluster
{
/// A naive blocking queue implementation.
/// Revisit this

template <typename T, typename Queue = std::list<T>>
class BlockingQueue
{
public:
    explicit BlockingQueue(size_t max_size_) : max_size(max_size_) { }

    /// add will be blocked if queue is full
    void add(const T & v)
    {
        {
            std::unique_lock lock{mu};
            if (queue.size() >= max_size)
                cv.wait(lock, [this] { return queue.size() < max_size; });

            queue.push_back(v);
            assert(queue.size() <= max_size);
        }

        /// Notify front() we have value
        cv.notify_one();
    }

    void add(T && v)
    {
        {
            std::unique_lock lock{mu};

            if (queue.size() >= max_size)
                cv.wait(lock, [this] { return queue.size() < max_size; });

            queue.push_back(std::move(v));
            assert(queue.size() <= max_size);
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();
    }

    /// \return current size after add and if passed value is added
    std::pair<size_t, bool> tryAdd(T && v)
    {
        size_t siz = 0;
        {
            std::unique_lock lock{mu};

            if (queue.size() == max_size)
                return {max_size, false};

            queue.push_back(std::move(v));
            assert(queue.size() <= max_size);
            siz = queue.size();
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();

        return {siz, true};
    }

    bool add(T && v, int64_t timeout_ms)
    {
        {
            std::unique_lock lock{mu};

            if (queue.size() >= max_size)
            {
                auto status = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return queue.size() < max_size; });
                if (!status)
                    return false;
            }

            queue.push_back(std::move(v));
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
            std::unique_lock lock{mu};

            if (queue.size() >= max_size)
                cv.wait(lock, [this] { return queue.size() < max_size; });

            queue.emplace_back(std::forward<Args>(args)...);
            assert(queue.size() <= max_size);
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();
    }

    template <typename... Args>
    bool tryEmplace(Args &&... args)
    {
        {
            std::unique_lock lock{mu};

            if (queue.size() == max_size)
                return false;

            queue.emplace_back(std::forward<Args>(args)...);
            assert(queue.size() <= max_size);
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();

        return true;
    }

    template <typename... Args>
    bool tryEmplace(int64_t timeout_ms, Args &&... args)
    {
        {
            std::unique_lock lock{mu};

            if (queue.size() >= max_size)
            {
                auto status = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return queue.size() < max_size; });
                if (!status)
                    return false;
            }

            queue.emplace_back(std::forward<Args>(args)...);
            assert(queue.size() <= max_size);
        }

        /// Notify one waiting thread we have one item to consume
        cv.notify_one();

        return true;
    }


    /// get and pop front
    T take()
    {
        std::unique_lock lock{mu};

        if (queue.empty())
            cv.wait(lock, [this] { return !queue.empty(); });

        assert(!queue.empty());
        T t{std::move(queue.front())};
        queue.pop_front();

        /// Manually unlocking is done before notifying to avoid waking up
        /// the waiting thread only to block again
        lock.unlock();

        /// Notify push/emplace, there is empty slot
        cv.notify_one();

        return t;
    }

    /// get and pop front if not timeout
    /// return empty if timeout
    std::optional<T> take(int64_t timeout_ms)
    {
        std::unique_lock lock{mu};

        if (queue.empty())
        {
            auto status = cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); });
            if (!status)
                return {};
        }

        assert(!queue.empty());
        T t{std::move(queue.front())};
        queue.pop_front();

        /// Manually unlocking is done before notifying to avoid waking up
        /// the waiting thread only to block again
        lock.unlock();

        /// Notify push/emplace, there is empty slot
        cv.notify_one();

        return t;
    }

    std::optional<T> tryTake()
    {
        std::unique_lock lock{mu};

        if (queue.empty())
            return {};

        T t{std::move(queue.front())};
        queue.pop_front();

        /// Manually unlocking is done before notifying to avoid waking up
        /// the waiting thread only to block again
        lock.unlock();

        /// Notify push/emplace, there is empty slot
        cv.notify_one();

        return t;
    }

    /// Get front. If queue is empty, wait forever for one
    const T & peek() const
    {
        std::unique_lock lock{mu};

        if (queue.empty())
            cv.wait(lock, [this] { return !queue.empty(); });

        assert(!queue.empty());
        return queue.front();
    }

    Queue drain()
    {
        Queue r;
        {
            std::unique_lock lock{mu};
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

    Queue drain(int64_t timeout_ms)
    {
        Queue r;
        {
            std::unique_lock lock{mu};

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

    size_t size() const noexcept
    {
        std::unique_lock lock{mu};
        return queue.size();
    }

    size_t maxSize() const noexcept { return max_size; }

    bool empty() const noexcept
    {
        std::unique_lock lock{mu};
        return queue.empty();
    }

private:
    const size_t max_size;

    std::condition_variable cv;
    mutable std::mutex mu;
    Queue queue;
};

template <typename T>
using BlockingQueuePtr = std::shared_ptr<BlockingQueue<T>>;
}
