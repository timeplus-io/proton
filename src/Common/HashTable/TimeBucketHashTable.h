#pragma once

/** A different two-level hash table for streaming processing only and in that scenario
  * the first group by key is always a timestamp window begin or window end.
  * Existing single level hashtable can't be converted to this two-level hash table
  */

#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>

template <size_t initial_size_degree = 8>
struct TimeBucketHashTableGrower : public HashTableGrower<initial_size_degree>
{
    /// Increase the size of the hash table.
    void increaseSize() { this->size_degree += this->size_degree >= 15 ? 1 : 2; }
};


/**
 * why need WindowOffset? what is it?
 * In query such as 'select ... from tumble(stream, 5s) group by window_start, col', if the toatal length of group by key are fixed,
 * and the col are nullable columns, in function 'packFixed', it will put the KeysNullMap(indicates which column of this row of data is null) in the front of the key,
 * then put the window time key and other group by key behind it.But in TimeBucketHashTable::windowKey, we assume the window time key is in the front of the key, 
 * The key's layout is like:
 *  |           key                |
 *  +-----------------+------------+
 *  | col, window time| KeysNullMap|
 *  +-----------------+------------+ low bit
 *                    |WindowOffset|
 * 
 * so we need to add a WindowOffset to indicate the length of the KeysNullMap, then we can get the window time key correctly.
 * PS: The WindowOffset will only work in this situation(group by window_start and other nullable column), other situation will not be 0, and it will not affect the result.
*/
template <
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator,
    typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>,
    size_t WindowOffset = 0>
class TimeBucketHashTable : private boost::noncopyable, protected Hash /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TimeBucketHashTable;

    size_t win_key_size;

public:
    using Impl = ImplTable;

    static constexpr char HEAD_SEPARATOR = ',';
    static constexpr char BUCKET_SEPARATOR = ';';
    static constexpr char KEY_VALUE_SEPARATOR = ':';
    static constexpr char END_BUCKET_MARKER = '`';

    size_t hash(const Key & x) const { return Hash::operator()(x); }

    template <typename T>
    ALWAYS_INLINE Int64 windowKey(T key)
    {
        /// window time key is always: 4 or 8 bytes
        /// window time key are always lower bits of integral type of T
        /// key & 0xFFFF or 0xFFFFFFFF or 0xFFFFFFFFFFFFFFFF

        return (key >> (8 * WindowOffset)) & ((0xFFull << ((win_key_size - 1) << 3)) + ((1ull << ((win_key_size - 1) << 3)) - 1));
    }

    ALWAYS_INLINE Int64 windowKey(StringRef key)
    {
        /// deserialize the first win_key_size bytes
        if (win_key_size == 8)
        {
            assert(key.size > 8);
            return unalignedLoad<Int64>(key.data);
        }
        else if (win_key_size == 4)
        {
            assert(key.size > 4);
            return unalignedLoad<UInt32>(key.data);
        }

        UNREACHABLE();
    }

protected:
    typename Impl::iterator beginOfNextNonEmptyBucket(Int64 & bucket)
    {
        auto it = impls.upper_bound(bucket);
        if (it != impls.end())
        {
            bucket = it->first;
            return it->second.begin();
        }

        bucket = 0;
        return sentinel.end();
    }

    typename Impl::const_iterator beginOfNextNonEmptyBucket(Int64 & bucket) const
    {
        auto it = impls.upper_bound(bucket);
        if (it != impls.end())
        {
            bucket = it->first;
            return it->second.begin();
        }

        bucket = 0;
        return sentinel.end();
    }

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    /// FIXME, choose a better perf data structure
    /// Usually we don't have too many time buckets
    std::map<Int64, Impl> impls;
    std::unordered_map<Int64, bool/*updated*/> updated_buckets;
    Impl sentinel;

    TimeBucketHashTable() { }

    void setWinKeySize(size_t win_key_size_)
    {
        assert(win_key_size_ == 4 || win_key_size_ == 8);
        win_key_size = win_key_size_;
    }

    /// Copy the data from another (normal) hash table. It should have the same hash function.
    template <typename Source>
    TimeBucketHashTable(const Source & src)
    {
        typename Source::const_iterator it = src.begin();

        for (; it != src.end(); ++it)
        {
            insert(it->getValue());
        }
    }

    class iterator
    {
        Self * container{};
        Int64 bucket{};
        typename Impl::iterator current_it{};

        friend class StreamingTwoLevelHashTable;

        iterator(Self * container_, Int64 bucket_, typename Impl::iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_)
        {
        }

    public:
        iterator() { }

        bool operator==(const iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
        bool operator!=(const iterator & rhs) const { return !(*this == rhs); }

        iterator & operator++()
        {
            ++current_it;
            if (current_it == container->impls[bucket].end())
            {
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        Cell & operator*() const { return *current_it; }
        Cell * operator->() const { return current_it.getPtr(); }

        Cell * getPtr() const { return current_it.getPtr(); }
        size_t getHash() const { return current_it.getHash(); }
    };

    class const_iterator
    {
        Self * container{};
        Int64 bucket{};
        typename Impl::const_iterator current_it{};

        const_iterator(Self * container_, Int64 bucket_, typename Impl::const_iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_)
        {
        }

    public:
        const_iterator() { }
        const_iterator(const iterator & rhs) : container(rhs.container), bucket(rhs.bucket), current_it(rhs.current_it) { }

        bool operator==(const const_iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
        bool operator!=(const const_iterator & rhs) const { return !(*this == rhs); }

        const_iterator & operator++()
        {
            ++current_it;
            if (current_it == container->impls[bucket].end())
            {
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        const Cell & operator*() const { return *current_it; }
        const Cell * operator->() const { return current_it->getPtr(); }

        const Cell * getPtr() const { return current_it.getPtr(); }
        size_t getHash() const { return current_it.getHash(); }
    };

    const_iterator begin() const
    {
        Int64 buck = 0;
        typename Impl::const_iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return {this, buck, impl_it};
    }

    iterator begin()
    {
        Int64 buck = 0;
        typename Impl::iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return {this, buck, impl_it};
    }

    const_iterator end() const { return {this, 0, sentinel.end()}; }
    iterator end() { return {this, 0, sentinel.end()}; }

    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        size_t hash_value = hash(Cell::getKey(x));

        std::pair<LookupResult, bool> res;
        emplace(Cell::getKey(x), res.first, res.second, hash_value);

        if (res.second)
            insertSetMapped(res.first->getMapped(), x);

        return res;
    }

    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` values if you inserted a new key,
      * since when destroying a hash table, the destructor will be invoked for it!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        size_t hash_value = hash(keyHolderGetKey(key_holder));
        emplace(key_holder, it, inserted, hash_value);
    }

    /// Same, but with a precalculated values of hash function.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash_value)
    {
        auto window = windowKey(keyHolderGetKey(key_holder));
        impls[window].emplace(key_holder, it, inserted, hash_value);
        updated_buckets[window] = true; /// updated
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value)
    {
        auto window = windowKey(x);
        return impls[window].find(x, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }

    void write(DB::WriteBuffer & wb) const
    {
        /// Write header
        DB::writeIntBinary(impls.size(), wb);

        for (const auto & p : impls)
        {
            DB::writeIntBinary(p.first);
            p.second.write(wb);
            DB::writeBinary(updated_buckets[p.first], wb);
        }
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        /// Write header
        DB::writeIntText(impls.size(), wb);
        DB::writeChar(HEAD_SEPARATOR, wb);

        Int32 i = 0;
        for (const auto & p : impls)
        {
            if (i != 0)
                DB::writeChar(BUCKET_SEPARATOR, wb);

            ++i;

            /// Write key and key-value separator
            DB::writeIntText(p.first, wb);
            DB::writeChar(KEY_VALUE_SEPARATOR, wb);
            /// <impl,updated>
            DB::writeChar('<', wb);
            p.second.writeText(wb);
            DB::writeChar(',', wb);
            DB::writeBoolText(updated_buckets[p.first], wb);
            DB::writeChar('>', wb);
        }
        DB::writeChar(END_BUCKET_MARKER, wb);
    }

    void read(DB::ReadBuffer & rb)
    {
        /// Read header
        size_t total = 0;
        DB::readIntBinary(total, rb);

        for (size_t i = 0; i < total; ++i)
        {
            size_t key = 0;
            DB::readIntBinary(key, rb);
            assert(key != 0);
            assert(!impls.contains(key));
            impls[key].read(rb);
            DB::readBinary(updated_buckets[key], rb);
        }
    }

    void readText(DB::ReadBuffer & rb)
    {
        /// Read header
        size_t total = 0;
        DB::readIntText(total, rb);
        DB::assertChar(HEAD_SEPARATOR, rb);

        for (size_t i = 0; i < total; ++i)
        {
            if (i != 0)
                DB::assertChar(BUCKET_SEPARATOR, rb);

            /// Read key and key-value separator
            size_t key = 0;
            DB::readIntText(key, rb);
            DB::assertChar(KEY_VALUE_SEPARATOR, rb);

            assert(key != 0);
            assert(!impls.contains(key));
            /// <impl,updated>
            DB::assertChar('<', rb);
            impls[key].readText(rb);
            DB::assertChar(',', rb);
            DB::readBoolText(updated_buckets[key], rb);
            DB::assertChar('>', rb);
        }
        DB::assertChar(END_BUCKET_MARKER, rb);
    }

    size_t size() const
    {
        size_t res = 0;
        for (const auto & p : impls)
            res += p.second.size();
        return res;
    }

    bool empty() const { return impls.empty(); }

    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (const auto & p : impls)
            res += p.second.getBufferSizeInBytes();

        return res;
    }

    size_t getBufferSizeInCells() const
    {
        size_t res = 0;
        for (const auto & p : impls)
            res += p.second.getBufferSizeInCells();

        return res;
    }

    /// Keep the latest X windows. If there is a gap in between, we still need clean the old window if they are X * interval
    /// after the current watermark
    /// Return {removed, last_removed_watermark, remaining_size}
    template <typename MappedDestroyFunc>
    std::tuple<size_t, Int64, size_t>
    removeBucketsBefore(Int64 max_bucket, MappedDestroyFunc && mapped_destroy)
    {
        Int64 last_removed_watermark = 0;
        size_t removed = 0;

        for (auto it = impls.begin(), it_end = impls.end(); it != it_end;)
        {
            if (it->first <= max_bucket)
            {
                it->second.forEachMapped(mapped_destroy);
                it->second.clearAndShrink();

                last_removed_watermark = it->first;
                ++removed;

                updated_buckets.erase(it->first);
                it = impls.erase(it);
            }
            else
                break;
        }

        auto new_size = impls.size();
        return {removed, last_removed_watermark, new_size};
    }

    std::vector<Int64> bucketsBefore(Int64 max_bucket) const
    {
        std::vector<Int64> buckets;
        buckets.reserve(10);

        for (const auto & time_map : impls)
        {
            if (time_map.first <= max_bucket)
                buckets.push_back(time_map.first);
            else
                break;
        }

        return buckets;
    }

    std::vector<Int64> buckets() const
    {
        std::vector<Int64> buckets;
        buckets.reserve(impls.size());

        for (const auto & time_map : impls)
            buckets.push_back(time_map.first);

        return buckets;
    }

    bool isBucketUpdated(Int64 bucket_) const
    {
        auto it = updated_buckets.find(bucket_);
        if (it != updated_buckets.end())
            return it->second;

        return false;
    }

    void resetUpdatedBucket(Int64 bucket_)
    {
        auto it = updated_buckets.find(bucket_);
        if (it != updated_buckets.end())
            it->second = false;
    }

    void writeUpdatedBuckets(DB::WriteBuffer & wb) const
    {
        DB::writeVarUInt(updated_buckets.size(), wb);
        for (const auto & [bucket, updated] : updated_buckets)
        {
            DB::writeIntBinary(bucket, wb);
            DB::writeBinary(updated, wb);
        }
    }

    void readUpdatedBuckets(DB::ReadBuffer & rb)
    {
        size_t size = 0;
        DB::readVarUInt(size, rb);
        updated_buckets.clear();
        Int64 bucket = 0;
        bool updated = false;
        for (size_t i = 0; i < size; ++i)
        {
            DB::readIntBinary(bucket, rb);
            DB::readBinary(updated, rb);
            updated_buckets.emplace(bucket, updated);
        }
    }
};
