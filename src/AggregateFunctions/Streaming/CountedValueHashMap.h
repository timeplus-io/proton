#pragma once

#include <base/StringRef.h>
#include <Common/ArenaUtils.h>
#include <Common/ArenaWithFreeLists.h>

#include <absl/container/flat_hash_map.h>

#include <unordered_map>

#include <stdexcept>

namespace DB
{
namespace Streaming
{
template <typename T>
struct CountedValueHashmapArena
{
    CountedValueHashmapArena() = default;
    T emplace(T key) { return std::move(key); }
    void free(const T & /*key*/) { }
};

/*
 * Specialized storage for StringRef with a freelist arena->
 * Keys of this type that are retained on insertion must be serialized into local storage,
 * otherwise the reference would be invalid after the processed block is released.
 */
template <>
struct CountedValueHashmapArena<StringRef>
{
    CountedValueHashmapArena() = default;
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

/// CountedValueHashMap maintain count for each key with no maximum capacity
template <typename T, typename KeyCompare = void>
class CountedValueHashMap
{
public:
    using Compare = std::conditional_t<std::is_void_v<KeyCompare>, std::equal_to<T>, KeyCompare>;
    using FlatHashMap = absl::flat_hash_map<T, uint32_t, DefaultHash<T>, Compare>;
    using STDHashMap = std::unordered_map<T, uint32_t, DefaultHash<T>, Compare>;
    using Map = std::conditional_t<std::is_nothrow_copy_constructible<Compare>::value, FlatHashMap, STDHashMap>;
    using size_type = typename Map::size_type;
    using key_type = T;

    CountedValueHashMap() = default;

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

    void merge(const CountedValueHashMap & rhs) { merge<true>(rhs); }

    /// After merge, `rhs` will be empty
    void merge(CountedValueHashMap & rhs)
    {
        merge<false>(rhs);
        rhs.clear();
    }

    void clear()
    {
        m.clear();
        arena = std::make_unique<CountedValueHashmapArena<T>>();
    }

    size_type size() const { return m.size(); }

    bool empty() const { return m.empty(); }

    void swap(CountedValueHashMap & rhs)
    {
        m.swap(rhs.m);
        std::swap(arena, rhs.arena);
    }

    auto begin() { return m.begin(); }
    auto begin() const { return m.begin(); }

    auto end() { return m.end(); }
    auto end() const { return m.end(); }

    CountedValueHashmapArena<T> & getArena() { return *arena; }

    static CountedValueHashMap & merge(CountedValueHashMap & lhs, CountedValueHashMap & rhs)
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

        assert(!rhs.empty() && !empty());

        /// Directly loop all elements as there's no order nor capacity
        for (auto src_iter = rhs.m.begin(); src_iter != rhs.m.end(); ++src_iter)
        {
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
                m.emplace(arena->emplace(src_iter->first), 1);
            else
                m.emplace(arena->emplace(std::move(src_iter->first)), src_iter->second);
        }
    }

    inline void clearAndClone(const CountedValueHashMap & rhs)
    {
        clear();

        /// Copy over all elements from rhs
        for (auto src_iter = rhs.begin(); src_iter != rhs.end(); ++src_iter)
            m.emplace(arena->emplace(src_iter->first), src_iter->second);
    }

    inline void clearAndSwap(CountedValueHashMap & rhs)
    {
        clear();
        swap(rhs);
    }

private:
    std::unique_ptr<CountedValueHashmapArena<T>> arena;
    Map m;
};
}
}
