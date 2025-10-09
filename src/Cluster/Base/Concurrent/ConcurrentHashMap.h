#pragma once

#include <absl/container/flat_hash_map.h>
#include <boost/noncopyable.hpp>

#include <shared_mutex>

namespace cluster
{
/// This is a naive concurrent hash map implementation and so far it shall work just good enough
/// Future exploration: folly ConcurrentHashMap, libcuckoo etc

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
class ConcurrentHashMap : private boost::noncopyable
{
public:
    ConcurrentHashMap() { }

    template <typename InputIt>
    ConcurrentHashMap(InputIt first, InputIt last) : hash_map(first, last)
    {
    }

    bool insert(const std::pair<const K, V> & elem)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.insert(elem);
        return inserted;
    }

    bool insert(std::pair<const K, V> && elem)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.insert(std::move(elem));
        return inserted;
    }

    /// \return a pair of V and bool. If bool is true means the element is
    /// inserted otherwise it is assigned. V is a copy of inserted value
    template <typename M>
    bool insertOrAssign(const K & k, M && obj)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.insert_or_assign(k, std::move(obj));
        return inserted;
    }

    template <typename M>
    bool insertOrAssign(K && k, M && obj)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.insert_or_assign(std::move(k), std::move(obj));
        return inserted;
    }

    template <typename InputIt>
    void insert(InputIt first, InputIt last)
    {
        std::unique_lock guard{hlock};
        hash_map.insert(first, last);
    }

    template <typename... Args>
    bool emplace(Args &&... args)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.emplace(std::forward<Args>(args)...);
        return inserted;
    }

    template <typename... Args>
    bool tryEmplace(const K & k, Args &&... args)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.try_emplace(k, std::forward<Args>(args)...);
        return inserted;
    }

    template <typename... Args>
    bool tryEmplace(K && k, Args &&... args)
    {
        std::unique_lock guard{hlock};
        auto [_, inserted] = hash_map.try_emplace(std::move(k), std::forward<Args>(args)...);
        return inserted;
    }

    size_t erase(const K & k)
    {
        std::unique_lock guard{hlock};
        return hash_map.erase(k);
    }

    template <typename Key>
    size_t erase(Key && k)
    {
        std::unique_lock guard{hlock};
        return hash_map.erase(std::move(k));
    }

    template <typename Pred>
    size_t eraseIf(Pred pred)
    {
        std::unique_lock guard{hlock};
        return std::erase_if(hash_map, pred);
    }

    std::vector<std::pair<K, V>> items() const
    {
        std::vector<std::pair<K, V>> results;

        {
            std::shared_lock guard{hlock};
            results.reserve(hash_map.size());

            for (const auto & item : hash_map)
                results.push_back(item);
        }

        return results;
    }

    std::vector<V> values() const
    {
        std::vector<V> results;

        {
            std::shared_lock guard{hlock};
            results.reserve(hash_map.size());

            for (const auto & item : hash_map)
                results.push_back(item.second);
        }

        return results;
    }

    /// Apply `func` to every element in the hash map
    void apply(std::function<void(const std::pair<K, V> &)> func) const
    {
        std::shared_lock guard{hlock};
        for (const auto & item : hash_map)
            func(item);
    }

    //    void apply(const std::function<void(const std::pair<K, V> &)> & func) const
    //    {
    //        std::shared_lock guard{hlock};
    //        for (const auto & item : hash_map)
    //            func(item);
    //    }
    //    void apply(std::function<void(const std::pair<K, V> &)> && func) const
    //    {
    //        std::shared_lock guard{hlock};
    //        for (const auto & item : hash_map)
    //            func(item);
    //    }

    void clear()
    {
        std::unique_lock guard{hlock};
        hash_map.clear();
    }

    bool contains(const K & k) const noexcept
    {
        std::shared_lock guard{hlock};
        return hash_map.contains(k);
    }

    template <typename Key>
    bool contains(const Key & k) const noexcept
    {
        std::shared_lock guard{hlock};
        return hash_map.contains(k);
    }

    /// return true if it contains `k` and `v` will be assigned
    /// return false if it doesn't contains `k` and `v` will stay unassigned
    bool at(const K & k, V & v) const
    {
        std::shared_lock guard{hlock};
        auto it = hash_map.find(k);
        if (it != hash_map.end())
        {
            v = it->second;
            return true;
        }
        else
            return false;
    }

    size_t size() const noexcept
    {
        std::shared_lock guard{hlock};
        return hash_map.size();
    }

    bool empty() const noexcept
    {
        std::shared_lock guard{hlock};
        return hash_map.empty();
    }

private:
    mutable std::shared_mutex hlock;
    absl::flat_hash_map<K, V, Hash, KeyEqual> hash_map;
};

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using ConcurrentHashMapPtr = std::shared_ptr<ConcurrentHashMap<K, V, Hash, KeyEqual>>;
}
