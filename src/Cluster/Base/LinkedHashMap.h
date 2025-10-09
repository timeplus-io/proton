#pragma once

#include <absl/container/flat_hash_map.h>

namespace cluster
{
/// A HashMap which maintains the key insertion order
template <typename K, typename V>
class LinkedHashMap final
{
public:
    struct KeyValue
    {
        KeyValue(K && k_, V && v_) : k(std::move(k_)), v(std::move(v_)) { }

        KeyValue(const K & k_, const V & v_) : k(k_), v(v_) { }

        K k;
        V v;
    };

private:
    using ElementIter = std::list<KeyValue>::iterator;

public:
    /// Insert or assign
    /// \return true if it is newly inserted, otherwise false
    bool insert(const K & k, const V & v)
    {
        bool new_key = false;
        auto iter = index.find(k);
        if (iter != index.end())
        {
            key_values.erase(iter->second);
            key_values.emplace_front(k, v);
            iter->second = key_values.begin();
        }
        else
        {
            key_values.emplace_front(k, v);
            auto [_, inserted] = index.emplace(k, key_values.begin());
            assert(inserted);
            new_key = inserted;
        }

        assert(key_values.size() == index.size());
        return new_key;
    }

    /// Insert or assign
    /// \return true if it is newly inserted, otherwise false
    bool insert(K && k, V && v)
    {
        bool new_key = false;
        auto iter = index.find(k);
        if (iter != index.end())
        {
            key_values.erase(iter->second);
            key_values.emplace_front(std::move(k), std::move(v));
            iter->second = key_values.begin();
        }
        else
        {
            key_values.emplace_front(K{k}, std::move(v));
            auto [_, inserted] = index.emplace(std::move(k), key_values.begin());
            assert(inserted);
            new_key = inserted;
        }

        assert(key_values.size() == index.size());
        return new_key;
    }

    /// \return 1 if k is found and erased, otherwise return 0
    template <typename KK>
    size_t erase(const KK & k)
    {
        auto iter = index.find(k);
        if (iter != index.end())
        {
            key_values.erase(iter->second);
            index.erase(iter);
            assert(key_values.size() == index.size());
            return 1;
        }
        else
            return 0;
    }

    /// \return next element iterator right after :iter.
    /// If there is no element after :iter, iter end will be returned
    ElementIter erase(ElementIter iter)
    {
        auto index_iter = index.find(iter->k);
        assert(index_iter != index.end());
        index.erase(index_iter);
        return key_values.erase(iter);
    }

    const auto & earliest() const
    {
        assert(!key_values.empty());
        return key_values.back();
    }

    const auto & latest() const
    {
        assert(!key_values.empty());
        return key_values.front();
    }

    /// Heterogeneous lookup
    template <class KK>
    auto find(const KK & k)
    {
        auto iter = index.find(k);
        if (iter != index.end())
            return iter->second;

        return key_values.end();
    }

    /// From latest to earliest
    auto begin() const { return key_values.begin(); }
    auto end() const { return key_values.end(); }

    /// From latest to earliest
    auto begin() { return key_values.begin(); }
    auto end() { return key_values.end(); }

    /// From earliest to latest
    auto rbegin() const { return key_values.rbegin(); }
    auto rend() const { return key_values.rend(); }

    /// From earliest to latest
    auto rbegin() { return key_values.rbegin(); }
    auto rend() { return key_values.rend(); }

    bool empty() const noexcept { return key_values.empty(); }
    size_t size() const noexcept { return key_values.size(); }

private:
    std::list<KeyValue> key_values;

    absl::flat_hash_map<K, ElementIter> index;
};
}
