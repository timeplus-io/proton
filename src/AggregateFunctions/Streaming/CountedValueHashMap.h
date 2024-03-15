#pragma once

#include <base/StringRef.h>
#include <Common/ArenaUtils.h>
#include <Common/ArenaWithFreeLists.h>

#include <stdexcept>
#include <absl/container/flat_hash_map.h>
#include <boost/unordered_map.hpp>

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

/// CountedValueHashMap maintain count for each key with maximum capacity
/// When capacity hits the max capacity threshold, it will adjust the max_load_factor to store more elements
template <typename T, typename KeyEq = void>
class CountedValueHashMap
{
public:
    using Eq = std::conditional_t<std::is_void_v<KeyEq>, std::equal_to<T>, KeyEq>;

    /// NOTE: Generally we prefer to use absl::btree_map, but it requires `Compare` is nothrow copy constructible, so if not, we use std::map
    using FlatHashMap = absl::flat_hash_map<T, uint32_t, absl::Hash<T>, Eq>;
    using STDUnorderedMap = std::unordered_map<T, uint32_t, std::hash<T>, Eq>;
    using Map = std::conditional_t<std::is_nothrow_copy_constructible<Eq>::value, FlatHashMap, STDUnorderedMap>;
    using size_type = typename Map::size_type;

    CountedValueHashMap() : CountedValueHashMap(0) {}

    /**
     * @brief Construct a new Counted Value Hash Map object
     * @param max_size_ decide the maximum bucket count of the hash map, if max_size_ is 10, the bucket count may be 13(different hash map implementation may have different strategy).
     *                  Then the bucket count is unchangeable, but the maximum number of elements in the map may be changed by the max_load_factor(load_factor = count of elements / bucket count).
     *                  Why you just assign the bucket count, because there is no API to set it.
     * @param eq the key equal function
     */
    explicit CountedValueHashMap(size_type reserve_size, Eq && eq = Eq{}) : arena(std::make_unique<CountedValueArena<T>>()), m(reserve_size)
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
        if (auto iter = m.find(v); iter != m.end())
        {
            ++iter->second;
            return iter;
        }
        else
        {
            auto [new_iter, inserted] = m.emplace(arena->emplace(std::move(v)), 1);
            assert(inserted);
            return new_iter;
        }
    }

    Map::iterator find(const T & v) { return m.find(v); }
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
    template <typename TT>
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
        arena = std::make_unique<CountedValueArena<T>>();
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

    CountedValueArena<T> & getArena() { return *arena; }

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

private:
    std::unique_ptr<CountedValueArena<T>> arena;
    Map m;
};
}
}
