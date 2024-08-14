#pragma once

#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashTable.h>

template <typename TMapped, typename Allocator = HashTableAllocator, template <typename...> typename ImplTable = StringHashMap>
class TwoLevelStringHashMap : public TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>
{
public:
    using Key = StringRef;
    using Self = TwoLevelStringHashMap;
    using Base = TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, Allocator>, ImplTable<TMapped, Allocator>>;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    /// proton: starts.
    template <typename Func>
    void ALWAYS_INLINE forEachValue(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachValue(func);
    }

    template <typename Func>
    void ALWAYS_INLINE forEachValueOfUpdatedBuckets(Func && func, bool reset_updated = false)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
        {
            if (this->isBucketUpdated(i))
            {
                this->impls[i].forEachValue(func);
                if (reset_updated)
                    this->resetUpdatedBucket(i);
            }
        }
    }

    template <typename Func, bool prefetch = false>
    void ALWAYS_INLINE mergeToViaEmplace(Self & that, Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].template mergeToViaEmplace<Func, prefetch>(that.impls[i], std::move(func));
    }
    /// proton: ends.

    TMapped & ALWAYS_INLINE operator[](const Key & x)
    {
        bool inserted;
        LookupResult it;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) TMapped();
        return it->getMapped();
    }
};
