#pragma once

#include <deque>
#include <shared_mutex>

#include <boost/noncopyable.hpp>

namespace nlog
{
/// A naive unbounded concurrent queue implementation.
/// Revisit this

template <typename T>
class UnboundedQueue : private boost::noncopyable
{
public:
    UnboundedQueue() { }

    void add(const T & v)
    {
        std::unique_lock guard{qlock};
        queue.push_back(v);
    }

    void add(T && v)
    {
        std::unique_lock guard{qlock};
        queue.push_back(std::move(v));
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        std::unique_lock guard{qlock};
        queue.emplace_back(std::forward<Args>(args)...);
    }

    /// get and pop if non-empty and setup `t` and return true
    /// otherwise return false leaving `t` unset
    bool take(T & t)
    {
        std::unique_lock guard{qlock};
        if (!queue.empty())
        {
            t = queue.front();
            queue.pop_front();
            return true;
        }
        return false;
    }

    bool peek(T & t) const
    {
        std::shared_lock guard{qlock};
        if (!queue.empty())
        {
            t = queue.front();
            return true;
        }
        return false;
    }

    void apply(std::function<void(const T &)> func) const
    {
        std::shared_lock guard{qlock};
        for (const auto & t : queue)
            func(t);
    }

    std::vector<T> snap() const
    {
        std::vector<T> result;

        std::shared_lock guard{qlock};

        result.reserve(queue.size());
        for (const auto & t : queue)
            result.push_back(t);

        return result;
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
    mutable std::shared_mutex qlock;
    std::deque<T> queue;
};
}
