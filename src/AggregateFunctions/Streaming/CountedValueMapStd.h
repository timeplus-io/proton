#pragma once

#include <map>

namespace DB
{
namespace Streaming
{
/// CountedValueMap maintain count for each key with maximum capacity
/// When capacity hits the max capacity threshold, it will delete
/// the minimum / maximum key in the map to maintain the capacity constrain
template <typename T, bool maximum>
class CountedValueMapStd
{
public:
    explicit CountedValueMapStd(size_t max_size_) : max_size(max_size_) { }

    bool insert(T v)
    {
        if (max_size > 0 && m.size() == max_size)
        {
            /// At capacity, this is an optimization
            /// fast ignore elements we don't want to maintain
            if constexpr (maximum)
            {
                /// Keep max N elements
                if (m.begin()->first > v)
                    /// `v` is less than min element in `m`
                    /// just ignore it
                    return false;
            }
            else
            {
                /// Keep min N elements
                if (m.rbegin()->first < v)
                    /// `v` is bigger than max element in `m`
                    /// just ignore it
                    return false;
            }
        }

        auto iter = m.find(v);
        if (iter != m.end())
        {
            ++iter->second;
            return true;
        }

        [[maybe_unused]] auto [_, inserted] = m.emplace(std::move(v), 1);
        assert(inserted);

        if (max_size > 0 && m.size() > max_size)
        {
            if constexpr (maximum)
                /// Remove the min element
                m.erase(m.begin());
            else
                /// Remove the max element
                m.erase(--m.end());
        }

        return true;
    }

    bool erase(const T & v)
    {
        auto iter = m.find(v);
        if (iter != m.end())
        {
            --iter->second;
            if (iter->second == 0)
                m.erase(iter);

            return true;
        }
        return false;
    }

    bool top(T & v) const
    {
        if (m.empty())
            return false;

        if constexpr (maximum)
            v = m.rbegin()->first;
        else
            v = m.begin()->first;

        return true;
    }

    void merge(CountedValueMapStd<T, maximum> & rhs)
    {
        /// Merge rhs to self and maintain the constrains
        for (auto src_iter = rhs.m.begin(); src_iter != rhs.m.end(); )
        {
            auto target_iter = m.find(src_iter->first);
            if (target_iter != m.end())
                target_iter->second += src_iter->second;
            else
                m.emplace(std::move(src_iter->first), 1);

            src_iter = rhs.m.erase(src_iter);
        }

        if constexpr (maximum)
        {
            auto iter = m.begin();
            while (m.size() > max_size)
                iter = m.erase(iter);
        }
        else
        {
            while (m.size() > max_size)
                m.erase(--m.end());
        }
    }

    size_t size() const
    {
        return m.size();
    }

    bool empty() const
    {
        return m.empty();
    }

    void swap(CountedValueMapStd<T, maximum> & rhs)
    {
        std::swap(max_size, rhs.max_size);
        m.swap(rhs.m);
    }

    auto begin() { return m.begin(); }
    auto begin() const { return m.begin(); }

    auto end() { return m.end(); }
    auto end() const { return m.end(); }

    static CountedValueMapStd & merge(CountedValueMapStd<T, maximum> & lhs, CountedValueMapStd<T, maximum> & rhs)
    {
        if (rhs.size() > lhs.size())
            lhs.swap(rhs);

        lhs.merge(rhs);
        return lhs;
    }

private:
    size_t max_size;
    std::map<T, uint32_t> m;
};
}
}
