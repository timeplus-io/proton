#pragma once

#include <base/StringRef.h>
#include <Common/ArenaUtils.h>
#include <Common/ArenaWithFreeLists.h>

#include <absl/container/btree_map.h>

#include <map>
#include <stdexcept>

namespace DB
{
namespace Streaming
{
template <typename T>
struct CountedValueArena
{
    CountedValueArena() = default;
    T emplace(T key) { return std::move(key); }
    void free(const T & /*key*/) { }
};

/*
 * Specialized storage for StringRef with a freelist arena->
 * Keys of this type that are retained on insertion must be serialized into local storage,
 * otherwise the reference would be invalid after the processed block is released.
 */
template <>
struct CountedValueArena<StringRef>
{
    CountedValueArena() = default;
    StringRef emplace(StringRef key) { return copyStringInArena(arena, key); }

    void free(StringRef key)
    {
        if (key.data)
            arena.free(const_cast<char *>(key.data), key.size);
    }

    ArenaWithFreeLists * getArenaWithFreeLists() { return &arena; }

private:
    ArenaWithFreeLists arena;
};

/// CountedValueMap maintain count for each key with maximum capacity
/// When capacity hits the max capacity threshold, it will delete
/// the minimum / maximum key in the map to maintain the capacity constrain
template <typename T, bool maximum, typename KeyCompare = void>
class CountedValueMap
{
public:
    using Compare = std::conditional_t<std::is_void_v<KeyCompare>, std::conditional_t<maximum, std::greater<T>, std::less<T>>, KeyCompare>;

    /// NOTE: Generally we prefer to use absl::btree_map, but it requires `Compare` is nothrow copy constructible, so if not, we use std::map
    using BTreeMap = absl::btree_map<T, uint32_t, Compare>;
    using STDMap = std::map<T, uint32_t, Compare>;
    using Map = std::conditional_t<std::is_nothrow_copy_constructible<Compare>::value, BTreeMap, STDMap>;
    using size_type = typename Map::size_type;

    CountedValueMap() = default;
    explicit CountedValueMap(size_type max_size_, Compare && comp = Compare{})
        : max_size(max_size_), arena(std::make_unique<CountedValueArena<T>>()), m(std::move(comp))
    {
    }

    /// This interface is used during deserialization of the map
    /// Assume `v` is not in the map
    bool insert(T v, uint32_t count)
    {
        [[maybe_unused]] auto [_, inserted] = m.emplace(arena->emplace(std::move(v)), count);
        return inserted;
    }

    /// Return the emplaced element iterator, if failed to emplace, return invalid iterator, `m.end()`
    Map::iterator emplace(T v)
    {
        if (atCapacity())
        {
            /// At capacity, this is an optimization
            /// fast ignore elements we don't want to maintain
            if (less(lastValue(), v))
                return m.end();
        }

        if (auto iter = m.find(v); iter != m.end())
        {
            ++iter->second;
            return iter;
        }
        else
        {
            /// Didn't find v in the map
            auto [new_iter, inserted] = m.emplace(arena->emplace(std::move(v)), 1);
            assert(inserted);

            eraseExtraElements();
            return new_iter;
        }
    }

    /// Return true if a new element was added.
    bool insert(T v)
    {
        auto iter = emplace(std::move(v));
        return iter != m.end();
    }

    /// To enable heterogeneous erase
    template <typename TT>
    bool erase(const TT & v)
    {
        auto iter = m.find(v);
        if (iter != m.end())
        {
            --iter->second;
            if (iter->second == 0)
            {
                arena->free(iter->first);
                m.erase(iter);
            }

            return true;
        }
        return false;
    }

    /// Return true if the element exists in the map.
    template<typename TT>
    bool contains(const TT & v) const
    {
        return m.find(v) != m.end();
    }

    bool firstValue(T & v) const
    {
        if (unlikely(m.empty()))
            return false;

        v = m.begin()->first;
        return true;
    }

    const T & firstValue() const
    {
        if (unlikely(m.empty()))
            throw std::logic_error("Call top on empty value map");

        return m.begin()->first;
    }

    bool lastValue(T & v) const
    {
        if (unlikely(m.empty()))
            return false;

        v = m.rbegin()->first;
        return true;
    }

    const T & lastValue() const
    {
        if (unlikely(m.empty()))
            throw std::logic_error("Call top on empty value map");

        return m.rbegin()->first;
    }

    void merge(const CountedValueMap & rhs) { merge<true>(rhs); }

    /// After merge, `rhs` will be empty
    void merge(CountedValueMap & rhs)
    {
        merge<false>(rhs);
        rhs.clear();
    }

    void clear()
    {
        m.clear();
        arena = std::make_unique<CountedValueArena<T>>();
    }

    inline bool atCapacity() const { return max_size > 0 && m.size() == max_size; }

    size_type capacity() const { return max_size; }

    void setCapacity(size_type max_size_) { max_size = max_size_; }

    size_type size() const { return m.size(); }

    bool empty() const { return m.empty(); }

    void swap(CountedValueMap & rhs)
    {
        std::swap(max_size, rhs.max_size);
        m.swap(rhs.m);
        std::swap(arena, rhs.arena);
    }

    auto begin() { return m.begin(); }
    auto begin() const { return m.begin(); }

    auto end() { return m.end(); }
    auto end() const { return m.end(); }

    CountedValueArena<T> & getArena() { return *arena; }

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

        /// The algorithm (for example minimum sorting):
        /// I) If lhs and rhs have no overlap value ranges : all values in lhs is greater or less than those in the rhs and
        ///    one of them are at capacity. There are 2 fast paths
        ///    1) if lhs is at capacity, and values in lhs are all greater than those in the rhs, there is nothing to merge. Just return
        ///    2) if rhs is at capacity, and values in rhs are all greater than those in the lhs, clear lhs and then copy everything from rhs.
        ///       Ideally it can be a lightweight swap, but since it is const rhs, we can't do that
        /// II) Slow path
        assert(!rhs.empty() && !empty());

        /// Optimize path : if lhs is at capacity and rhs has no overlap of lhs
        if (atCapacity())
        {
            /// If all values in lhs are less/greater (i.e. for minimum/maximum) than rhs
            /// we don't need any merge
            if (less(lastValue(), rhs.firstValue()))
                return;

            /// If all values in lhs are greater than rhs and rhs are at capacity as well
            if (rhs.atCapacity() && rhs.capacity() == capacity() && greater(firstValue(), rhs.lastValue()))
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
            if (greater(firstValue(), rhs.lastValue()))
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
            if (atCapacity() && less(lastValue(), src_iter->first))
                /// We reached maximum capacity and all other values from rhs will be
                /// greater than those already in lhs. Stop merging more
                break;

            doMerge<copy>(src_iter);
        }

        /**
         * @brief 
         * eg: self.m has key 4,5,6 and max_size is 5, and rhs.m has key 1, 2, 3, when merge 1, 2 in rhs.m to self.m,
         * self.m will be 1, 2, 4, 5, 6, although it reaches max_size, it will still merge the reset key in rhs.m to self.m
         * self.m will be 1, 2, 3, 4, 5.Then it will call eraseExtraElements() to remove the extra elements in the tail of self.m,
         * then self.m will be 1, 2, 3, 4, 5
         */
        eraseExtraElements();
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
                m.emplace(arena->emplace(src_iter->first), 1);
            else
                m.emplace(arena->emplace(std::move(src_iter->first)), src_iter->second);
        }
    }

    void eraseExtraElements()
    {
        if (max_size <= 0)
            return;

        while (m.size() > max_size)
        {
            auto last_elem = --m.end();
            arena->free(last_elem->first);
            m.erase(last_elem);
        }
    }

    inline void clearAndClone(const CountedValueMap & rhs)
    {
        clear();

        /// Copy over all elements from rhs
        for (auto src_iter = rhs.begin(); src_iter != rhs.end(); ++src_iter)
            m.emplace(arena->emplace(src_iter->first), src_iter->second);
    }

    inline void clearAndSwap(CountedValueMap & rhs)
    {
        clear();
        swap(rhs);
    }

    /// \returns: true means l < r for minimum order
    inline bool less(const T & l, const T & r) const { return m.key_comp()(l, r); }

    /// \returns: true means l > r for minimum order
    inline bool greater(const T & l, const T & r) const { return m.key_comp()(r, l); }

private:
    size_type max_size;
    std::unique_ptr<CountedValueArena<T>> arena;
    Map m;
};
}
}
