#pragma once

#include <parallel_hashmap/phmap.h>
#include <boost/noncopyable.hpp>

#include <functional>
#include <shared_mutex>
#include <vector>

namespace cluster
{
/// Thread-safe concurrent hash map backed by phmap::parallel_flat_hash_map.
/// Replaces the previous absl::flat_hash_map + single std::shared_mutex design.
/// Locking is now per-submap (16 submaps, each with its own std::shared_mutex),
/// so operations on keys in different submaps proceed fully in parallel.

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
class ConcurrentHashMap : private boost::noncopyable
{
public:
    using MapType = phmap::parallel_flat_hash_map<
        K, V, Hash, KeyEqual,
        phmap::priv::Allocator<std::pair<const K, V>>,
        4,                   // 2^4 = 16 submaps
        std::shared_mutex>;  // reader-writer lock per submap

    ConcurrentHashMap() { }

    template <typename InputIt>
    ConcurrentHashMap(InputIt first, InputIt last) : hash_map(first, last)
    {
    }

    bool insert(const std::pair<const K, V> & elem)
    {
        return hash_map.insert(elem).second;
    }

    bool insert(std::pair<const K, V> && elem)
    {
        return hash_map.insert(std::move(elem)).second;
    }

    /// \return a pair of V and bool. If bool is true means the element is
    /// inserted otherwise it is assigned. V is a copy of inserted value
    template <typename M>
    bool insertOrAssign(const K & k, M && obj)
    {
        return hash_map.insert_or_assign(k, std::forward<M>(obj)).second;
    }

    template <typename M>
    bool insertOrAssign(K && k, M && obj)
    {
        return hash_map.insert_or_assign(std::move(k), std::forward<M>(obj)).second;
    }

    template <typename InputIt>
    void insert(InputIt first, InputIt last)
    {
        hash_map.insert(first, last);
    }

    template <typename... Args>
    bool emplace(Args &&... args)
    {
        return hash_map.emplace(std::forward<Args>(args)...).second;
    }

    template <typename... Args>
    bool tryEmplace(const K & k, Args &&... args)
    {
        return hash_map.try_emplace(k, std::forward<Args>(args)...).second;
    }

    template <typename... Args>
    bool tryEmplace(K && k, Args &&... args)
    {
        return hash_map.try_emplace(std::move(k), std::forward<Args>(args)...).second;
    }

    size_t erase(const K & k)
    {
        return hash_map.erase(k);
    }

    template <typename Key>
    size_t erase(Key && k)
    {
        return hash_map.erase(std::move(k));
    }


    std::vector<std::pair<K, V>> items() const
    {
        std::vector<std::pair<K, V>> results;

        results.reserve(hash_map.size());
        hash_map.for_each([&results](const auto & item) {
            results.push_back(item);
        });

        return results;
    }

    std::vector<V> values() const
    {
        std::vector<V> results;

        results.reserve(hash_map.size());
        hash_map.for_each([&results](const auto & item) {
            results.push_back(item.second);
        });

        return results;
    }

    /// Apply `func` to every element in the hash map
    void apply(std::function<void(const std::pair<K, V> &)> func) const
    {
        hash_map.for_each([&func](const auto & item) {
            func(item);
        });
    }

    void clear()
    {
        hash_map.clear();
    }

    bool contains(const K & k) const noexcept
    {
        return hash_map.contains(k);
    }

    template <typename Key>
    bool contains(const Key & k) const noexcept
    {
        return hash_map.contains(k);
    }

    /// return true if it contains `k` and `v` will be assigned
    /// return false if it doesn't contains `k` and `v` will stay unassigned
    bool at(const K & k, V & v) const
    {
        return hash_map.if_contains(k, [&v](const auto & item) {
            v = item.second;
        });
    }

    size_t size() const noexcept
    {
        return hash_map.size();
    }

    bool empty() const noexcept
    {
        return hash_map.empty();
    }

private:
    MapType hash_map;
};

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ConcurrentHashMapPtr = std::shared_ptr<ConcurrentHashMap<K, V, Hash, KeyEqual>>;
}
