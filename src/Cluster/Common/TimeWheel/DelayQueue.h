#pragma once

#include <Cluster/Common/TimeWheel/TimerTaskList.h>

#include <condition_variable>
#include <optional>
#include <queue>


namespace cluster
{
class DelayQueue
{
public:
    using T = TimerTaskListPtr;
    using Clock = std::chrono::steady_clock;

    DelayQueue() = default;

    ~DelayQueue() noexcept { cv.notify_all(); }

    bool offer(T elem)
    {
        if (!elem)
            return false;

        {
            std::lock_guard lock(mutex_);

            /// Optimization: Fast path for empty queue
            if (queue_.empty())
            {
                queue_.push(std::move(elem));
                cv.notify_one(); /// Wake up any waiting consumers
                return true;
            }

            /// Compare with current top element
            const T & top = queue_.top();
            bool will_be_top = elem->getExpiration() < top->getExpiration();
            queue_.push(std::move(elem));

            /// Only notify if the new element becomes the top
            if (will_be_top)
                cv.notify_one();
        }
        return true;
    }

    std::optional<T> poll() noexcept
    {
        std::lock_guard lock(mutex_);
        return pollLocked();
    }

    /// Blocking poll with timeout
    /// @param timeout_ms Timeout in milliseconds
    /// @return The expired head element, or nullopt if timeout occurs
    std::optional<T> poll(int64_t timeout_ms) { return poll(std::chrono::milliseconds(timeout_ms)); }

    /// Blocking poll with chrono duration
    /// @param timeout Timeout duration
    /// @return The expired head element, or nullopt if timeout occurs
    std::optional<T> poll(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex_);

        const auto deadline = Clock::now() + timeout;

        while (true)
        {
            /// Try non-blocking poll first
            if (auto result = pollLocked())
                return result;

            if (queue_.empty())
            {
                /// Empty queue - wait until deadline
                if (cv.wait_until(lock, deadline) == std::cv_status::timeout)
                    return std::nullopt;

                continue;
            }

            /// Calculate delay for top element
            const T & first = queue_.top();
            auto delay = first->getDelay();

            /// Found expired element
            if (delay <= std::chrono::milliseconds(0))
            {
                auto result = std::move(queue_.top());
                queue_.pop();
                return result;
            }

            /// Check if we've exceeded timeout
            auto now = Clock::now();
            if (now >= deadline)
                return std::nullopt;

            auto remaining_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

            /// Wait for the shorter duration between delay and remaining timeout
            cv.wait_for(lock, std::min(delay, remaining_timeout));
        }

        return std::nullopt;
    }

private:
    /// Helper function for non-blocking poll operation
    std::optional<T> pollLocked()
    {
        if (queue_.empty())
            return std::nullopt;

        const T & first = queue_.top();
        if (first->getDelay() > std::chrono::milliseconds(0))
            return std::nullopt;

        auto result = std::move(queue_.top());
        queue_.pop();
        return result;
    }

    /// Compare elements based on absolute expiration time
    /// https://github.com/apache/kafka/blob/f2b19baee04edb02ece7b2b7e4c6b50b1301ac9c/server-common/src/main/java/org/apache/kafka/server/util/timer/TimerTaskList.java#L122-L130
    ///
    /// t=0:
    /// TaskA: delay=10, exp=10 < TaskB: delay=15, exp=15  -> A comes first
    ///
    /// then at t=8:
    /// TaskA: delay=2, exp=10 < TaskB: delay=7, exp=15    -> A still comes first
    ///
    /// the getDelay() and getExpiration() actually keep same order, the only difference is performance and semantic.
    struct DelayCompare
    {
        bool operator()(const T & left, const T & right) const noexcept { return left->getExpiration() > right->getExpiration(); }
    };

    mutable std::mutex mutex_;
    std::condition_variable cv;
    std::priority_queue<T, std::vector<T>, DelayCompare> queue_;
};
}
