#pragma once

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TimeBucketHashTable.h>

template <
    typename Key,
    typename Cell,
    typename Hash = DefaultHash<Key>,
    typename Grower = TimeBucketHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable,
    size_t WindowOffset = 0>
class TimeBucketHashMapTable
    : public TimeBucketHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, WindowOffset>
{
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using LookupResult = typename Impl::LookupResult;

    using TimeBucketHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>, WindowOffset>::TimeBucketHashTable;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto & p : this->impls)
            p.second.forEachMapped(func);
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        for (auto & p : this->impls)
            p.second.forEachValue(func);
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValueOfUpdatedBuckets(Func && func, bool reset_updated = false)
    {
        for (auto & p : this->impls)
        {
            if (this->isBucketUpdated(p.first))
            {
                p.second.forEachValue(func);
                if (reset_updated)
                    this->resetUpdatedBucket(p.first);
            }
        }
    }

    typename Cell::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) typename Cell::Mapped();

        return it->getMapped();
    }
};

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    size_t WindowOffset = 0,
    typename Grower = TimeBucketHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable>
using TimeBucketHashMap = TimeBucketHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable, WindowOffset>;

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TimeBucketHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable>
using TimeBucketHashMapWithSavedHash
    = TimeBucketHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable>;
