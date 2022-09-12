#pragma once

#include <absl/container/btree_map.h>

#include <iterator>
#include <stdexcept>

namespace DB
{
namespace Streaming
{
/// CountedArgValueMap maintain count for each key with maximum capacity
/// When capacity hits the max capacity threshold, it will delete
/// the minimum / maximum key in the map to maintain the capacity constrain
template <typename T, typename A, bool maximum>
class CountedArgValueMap
{
public:
    explicit CountedArgValueMap(int64_t max_size_) : max_size(max_size_) { }

    /// This interface is used during deserialization of the map
    /// Assume `v, arg` is not in the map
    bool insert(const T & v, A arg, uint32_t count)
    {
        auto iter = m.find(v);
        if (iter == m.end())
        {
            auto arg_counts = std::make_unique<ArgCounts>();
            arg_counts->emplace_back(std::move(arg), count);
            m.emplace(v, std::move(arg_counts));
        }
        else
        {
            iter->second->emplace_back(std::move(arg), count);
        }

        ++current_size;

        return true;
    }

    bool insert(const T & v, const A & arg)
    {
        if (atCapacity())
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
            /// Try to find if `arg` is in ArgCount vector
            auto arg_count_iter = std::find(iter->second->begin(), iter->second->end(), arg);
            if (arg_count_iter == iter->second->end())
            {
                iter->second->emplace_back(arg, 1);
                ++current_size;
            }
            else
                ++arg_count_iter->count;
        }
        else
        {
            /// Didn't find v in the map
            auto arg_counts = std::make_unique<ArgCounts>();
            arg_counts->emplace_back(arg, 1);
            [[maybe_unused]] auto [_, inserted] = m.emplace(v, std::move(arg_counts));
            assert(inserted);

            ++current_size;
        }

        eraseExtraElements();

        assert(current_size <= max_size);

        return true;
    }

    bool erase(const T & v, const A & arg)
    {
        auto iter = m.find(v);
        if (iter != m.end())
        {
            auto arg_counts_iter = std::find(iter->second->begin(), iter->second->end(), arg);
            if (arg_counts_iter != iter->second->end())
            {
                if (--arg_counts_iter->count == 0)
                {
                    iter->second->erase(arg_counts_iter);
                    --current_size;
                }

                if (iter->second->empty())
                    m.erase(iter);

                return true;
            }
        }
        return false;
    }

    bool firstArg(A & arg) const
    {
        if (m.empty())
            return false;

        if constexpr (maximum)
            arg = m.rbegin()->second->begin()->arg;
        else
            arg = m.begin()->second->begin()->arg;

        return true;
    }

    const A & firstArg() const
    {
        if (m.empty())
            throw std::logic_error("Call top on empty arg value map");

        if constexpr (maximum)
            return m.rbegin()->second->begin()->arg;
        else
            return m.begin()->second->begin()->arg;
    }

    const T & firstValue() const
    {
        if (m.empty())
            throw std::logic_error("Call top on empty arg value map");

        if constexpr (maximum)
            return m.rbegin()->first;
        else
            return m.begin()->first;
    }

    const T & lastValue() const
    {
        if (m.empty())
            throw std::logic_error("Call top on empty arg value map");

        if constexpr (maximum)
            return m.begin()->first;
        else
            return m.rbegin()->first;
    }

    void merge(const CountedArgValueMap & rhs) { merge<true>(rhs); }

    /// After merge, `rhs` will be modified and be empty
    void merge(CountedArgValueMap & rhs)
    {
        merge<false>(rhs);
        rhs.clear();
    }

    void setCapacity(int64_t max_size_) { max_size = max_size_; }

    int64_t capacity() const { return max_size; }

    int64_t size() const { return current_size; }

    bool empty() const { return m.empty(); }

    void clear()
    {
        current_size = 0;
        m.clear();
    }

    void swap(CountedArgValueMap<T, A, maximum> & rhs)
    {
        std::swap(current_size, rhs.current_size);
        std::swap(max_size, rhs.max_size);
        m.swap(rhs.m);
    }

    auto begin() { return m.begin(); }
    auto begin() const { return m.begin(); }

    auto end() { return m.end(); }
    auto end() const { return m.end(); }

    static CountedArgValueMap & merge(CountedArgValueMap & lhs, CountedArgValueMap & rhs)
    {
        if (rhs.size() > lhs.size())
            lhs.swap(rhs);

        lhs.merge(rhs);
        return lhs;
    }

    struct ArgCount
    {
        ArgCount(A && arg_, uint32_t count_) : arg(std::move(arg_)), count(count_) { }
        ArgCount(const A & arg_, uint32_t count_) : arg(arg_), count(count_) { }
        ArgCount() { }

        A arg{};
        uint32_t count{};

        bool operator==(const ArgCount & rhs) const { return arg == rhs.arg; }

        bool operator==(const A & arg_) const { return arg == arg_; }
    };

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
            /// Add `args` which are not in self
            auto iter_begin = target_iter->second->begin();
            auto iter_end = target_iter->second->end();
            for (auto & arg_count : *src_iter->second)
            {
                auto iter = std::find(iter_begin, iter_end, arg_count);
                if (iter != iter_end)
                {
                    iter->count += arg_count.count;
                }
                else
                {
                    if constexpr (copy)
                        target_iter->second->push_back(arg_count);
                    else
                        target_iter->second->push_back(std::move(arg_count));

                    ++current_size;
                }
            }
        }
        else
        {
            current_size += src_iter->second->size();

            /// FIXME, copy it over, may be too expensive, std::shared_ptr ?
            if constexpr (copy)
                m.emplace(src_iter->first, std::make_unique<ArgCounts>(*src_iter->second));
            else
                m.emplace(src_iter->first, std::move(src_iter->second));
        }
    }

    void eraseExtraElements()
    {
        if (max_size <= 0)
            return;

        /// Remove extra elements
        if constexpr (maximum)
        {
            auto iter = m.begin();
            while (current_size > max_size)
            {
                if (current_size >= max_size + static_cast<int64_t>(iter->second->size()))
                {
                    iter = m.erase(iter);
                    current_size -= iter->second->size();
                }
                else
                {
                    /// Erase a portion of ArgCounts
                    auto to_erase = current_size - max_size;
                    auto erase_iter_start = iter->second->begin();
                    std::advance(erase_iter_start, iter->second->size() - to_erase);
                    iter->second->erase(erase_iter_start, iter->second->end());
                    current_size -= to_erase;
                }
            }
        }
        else
        {
            while (current_size > max_size)
            {
                auto iter = --m.end();

                if (current_size >= max_size + static_cast<int64_t>(iter->second->size()))
                {
                    current_size -= iter->second->size();
                    m.erase(iter);
                }
                else
                {
                    /// Erase a portion of ArgCounts
                    auto to_erase = current_size - max_size;

                    auto erase_iter_start = iter->second->begin();
                    std::advance(erase_iter_start, iter->second->size() - to_erase);
                    iter->second->erase(erase_iter_start, iter->second->end());
                    current_size -= to_erase;
                }
            }
        }
    }

    inline void clearAndClone(const CountedArgValueMap & rhs)
    {
        clear();

        /// Copy over all elements from rhs
        for (auto src_iter = rhs.begin(); src_iter != rhs.end(); ++src_iter)
            m.emplace(src_iter->first, std::make_unique<ArgCounts>(*src_iter->second));

        current_size = rhs.current_size;
    }

    inline void clearAndSwap(CountedArgValueMap & rhs)
    {
        clear();
        swap(rhs);
    }

    inline bool atCapacity() const { return max_size > 0 && current_size == max_size; }

private:
    int64_t max_size;
    /// current unique (v, arg) combinations
    int64_t current_size = 0;

    using ArgCounts = std::vector<ArgCount>;
    using ArgCountsPtr = std::unique_ptr<ArgCounts>;

    absl::btree_map<T, ArgCountsPtr> m;
};
}
}
