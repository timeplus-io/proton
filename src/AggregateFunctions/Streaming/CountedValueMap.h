#pragma once

#include <absl/container/btree_map.h>

#include <stdexcept>

namespace DB
{
namespace Streaming
{
/// CountedValueMap maintain count for each key with maximum capacity
/// When capacity hits the max capacity threshold, it will delete
/// the minimum / maximum key in the map to maintain the capacity constrain
template <typename T, bool maximum>
class CountedValueMap
{
public:
    explicit CountedValueMap(int64_t max_size_) : max_size(max_size_) { }

    /// This interface is used during deserialization of the map
    /// Assume `v` is not in the map
    bool insert(T v, uint32_t count)
    {
        [[maybe_unused]] auto [_, inserted] = m.emplace(std::move(v), count);
        return inserted;
    }

    bool insert(const T & v)
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
        }
        else
        {
            /// Didn't find v in the map
            [[maybe_unused]] auto [_, inserted] = m.emplace(v, 1);
            assert(inserted);

            eraseExtraElements();
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

    bool firstValue(T & v) const
    {
        if (m.empty())
            return false;

        if constexpr (maximum)
            v = m.rbegin()->first;
        else
            v = m.begin()->first;

        return true;
    }

    const T & firstValue() const
    {
        if (m.empty())
            throw std::logic_error("Call top on empty value map");

        if constexpr (maximum)
            return m.rbegin()->first;
        else
            return m.begin()->first;
    }

    bool lastValue(T & v) const
    {
        if (m.empty())
            return false;

        if constexpr (maximum)
            v = m.begin()->first;
        else
            v = m.rbegin()->first;

        return true;
    }

    const T & lastValue() const
    {
        if (m.empty())
            throw std::logic_error("Call top on empty value map");

        if constexpr (maximum)
            return m.begin()->first;
        else
            return m.rbegin()->first;
    }

    void merge(const CountedValueMap & rhs) { merge<true>(rhs); }

    /// After merge, `rhs` will be empty
    void merge(CountedValueMap & rhs)
    {
        merge<false>(rhs);
        rhs.clear();
    }

    void clear() { m.clear(); }

    inline bool atCapacity() const { return max_size > 0 && m.size() == max_size; }

    int64_t capacity() const { return max_size; }

    void setCapacity(int64_t max_size_) { max_size = max_size_; }

    int64_t size() const { return m.size(); }

    bool empty() const { return m.empty(); }

    void swap(CountedValueMap & rhs)
    {
        std::swap(max_size, rhs.max_size);
        m.swap(rhs.m);
    }

    auto begin() { return m.begin(); }
    auto begin() const { return m.begin(); }

    auto end() { return m.end(); }
    auto end() const { return m.end(); }

    static CountedValueMap & merge(CountedValueMap & lhs, CountedValueMap & rhs)
    {
        if (rhs.size() > lhs.size())
            lhs.swap(rhs);

        lhs.merge(rhs);
        return lhs;
    }

private:
    template <bool copy, typename Map>
    void merge(Map & rhs)
    {
        if (rhs.empty())
            return;

        if (empty())
        {
            if constexpr (copy)
                return clearAndClone(rhs);
            else
                return swap(rhs);
        }

        if constexpr (maximum)
            mergeMaximum<copy>(rhs);
        else
            mergeMinimum<copy>(rhs);

        eraseExtraElements();
    }

    /// The algorithm:
    /// I) If lhs and rhs have no overlap value ranges : all values in lhs is greater or less than those in the rhs and
    ///    one of them are at capacity. There are 2 fast paths
    ///    1) if lhs is at capacity, and values in lhs are all greater than those in the rhs, there is nothing to merge. Just return
    ///    2) if rhs is at capacity, and values in rhs are all greater than those in the lhs, clear lhs and then copy everything from rhs.
    ///       Ideally it can be a lightweight swap, but since it is const rhs, we can't do that
    /// II) Slow path
    template <bool copy, typename Map>
    void mergeMinimum(Map & rhs)
    {
        static_assert(!maximum);

        assert(!rhs.empty() && !empty());

        /// Optimize path : if lhs is at capacity and rhs has no overlap of lhs
        if (atCapacity())
        {
            /// If all values in lhs are less than rhs
            /// we don't need any merge
            if (lastValue() <= rhs.firstValue())
                return;

            /// If all values in lhs are greater than rhs and rhs are at capacity as well
            if (rhs.atCapacity() && rhs.capacity() == capacity() && firstValue() > rhs.lastValue())
            {
                if constexpr (copy)
                    return clearAndClone(rhs);
                else
                    return clearAndSwap(rhs);
            }
        }

        if (rhs.atCapacity() && rhs.capacity() == capacity())
        {
            /// If all values in lhs are greater than rhs
            /// we can clear up lhs elements and copy over elements from rhs
            if (firstValue() > rhs.lastValue())
            {
                if constexpr (copy)
                    return clearAndClone(rhs);
                else
                    return clearAndSwap(rhs);
            }
        }

        /// Loop from min to max
        for (auto src_iter = rhs.m.begin(); src_iter != rhs.m.end(); ++src_iter)
        {
            if (atCapacity() && lastValue() < src_iter->first)
                /// We reached maximum capacity and all other values from rhs will be
                /// greater than those already in lhs. Stop merging more
                break;

            doMerge<copy>(src_iter);
        }
    }

    /// The algorithm:
    /// I) If lhs and rhs have no overlap value ranges : all values in lhs is greater or less than those in the rhs and
    ///    one of them are at capacity. There are 2 fast paths
    ///    1) if lhs is at capacity, and values in lhs are all less than those in the rhs, there is nothing to merge. Just return
    ///    2) if rhs is at capacity, and values in rhs are all less than those in the lhs, clear lhs and then copy everything from rhs.
    ///       Ideally it can be a lightweight swap, but since it is const rhs, we can't do that
    /// II) Slow path
    template <bool copy, typename Map>
    void mergeMaximum(Map & rhs)
    {
        static_assert(maximum);

        assert(!rhs.empty() && !empty());

        /// Optimize path : if lhs is at capacity and rhs has no overlap of lhs
        if (atCapacity())
        {
            /// If all values in lhs are greater than rhs
            /// we don't need any merge
            if (lastValue() >= rhs.firstValue())
                return;

            /// If all values in lhs are less than rhs and rhs are at capacity as well
            if (rhs.atCapacity() && rhs.capacity() == capacity() && firstValue() < rhs.lastValue())
            {
                if constexpr (copy)
                    return clearAndClone(rhs);
                else
                    return clearAndSwap(rhs);
            }
        }

        if (rhs.atCapacity() && rhs.capacity() == capacity())
        {
            /// If all values in lhs are less than rhs
            /// we can clear up lhs elements and copy over elements from rhs
            if (firstValue() < rhs.lastValue())
            {
                if constexpr (copy)
                    return clearAndClone(rhs);
                else
                    return clearAndSwap(rhs);
            }
        }

        /// Loop from max to min
        for (auto src_iter = rhs.m.rbegin(); src_iter != rhs.m.rend(); ++src_iter)
        {
            if (atCapacity() && lastValue() > src_iter->first)
                /// We reached maximum capacity and all other values from rhs will be
                /// less than those already in lhs. Stop merging more
                break;

            doMerge<copy>(src_iter);
        }
    }

    template <bool copy, typename Iter>
    void doMerge(Iter src_iter)
    {
        auto target_iter = m.find(src_iter->first);
        if (target_iter != m.end())
        {
            target_iter->second += src_iter->second;
        }
        else
        {
            if constexpr (copy)
                m.emplace(src_iter->first, 1);
            else
                m.emplace(std::move(src_iter->first), src_iter->second);
        }
    }

    void eraseExtraElements()
    {
        if (max_size <= 0)
            return;

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

    inline void clearAndClone(const CountedValueMap & rhs)
    {
        clear();

        /// Copy over all elements from rhs
        for (auto src_iter = rhs.begin(); src_iter != rhs.end(); ++src_iter)
            m.emplace(src_iter->first, src_iter->second);
    }

    inline void clearAndSwap(CountedValueMap & rhs)
    {
        clear();
        swap(rhs);
    }

private:
    int64_t max_size;
    absl::btree_map<T, uint32_t> m;
};
}
}
