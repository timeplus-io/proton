#pragma once

/** A different two-level hash table for streaming processing only and in that scenario
  * the first group by key is always a timestamp window begin or window end.
  * Existing single level hashtable can't be converted to this two-level hash table
  */

#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>

template <size_t initial_size_degree = 8>
struct StreamingTwoLevelHashTableGrower : public HashTableGrower<initial_size_degree>
{
    /// Increase the size of the hash table.
    void increaseSize() { this->size_degree += this->size_degree >= 15 ? 1 : 2; }
};

template <
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator,
    typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>>
class StreamingTwoLevelHashTable : private boost::noncopyable,
                                   protected Hash /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = StreamingTwoLevelHashTable;

    std::vector<size_t> key_sizes;

public:
    using Impl = ImplTable;

    static constexpr char HEAD_SEPARATOR = ',';
    static constexpr char BUCKET_SEPARATOR = ';';
    static constexpr char KEY_VALUE_SEPARATOR = ':';
    static constexpr char END_BUCKET_MARKER = '`';

    size_t hash(const Key & x) const { return Hash::operator()(x); }

    ALWAYS_INLINE size_t windowKey(UInt16 key)
    {
        /// window time key is always: 2, 4 or 8 bytes
        assert(key_sizes[0] == 2);
        return key;
    }

    ALWAYS_INLINE size_t windowKey(UInt32 key)
    {
        /// window time key is always: 2, 4 or 8 bytes
        if (key_sizes[0] == 4)
            return key;

        assert(key_sizes[0] == 2);
        return (key >> 16);
    }

    ALWAYS_INLINE size_t windowKey(UInt64 key)
    {
        /// window time key is always: 2, 4 or 8 bytes
        if (key_sizes[0] == 8)
            return key;

        return key >> (64 - (key_sizes[0] << 3));
    }

    ALWAYS_INLINE size_t windowKey(UInt128 key)
    {
        /// window time key is always: 2, 4 or 8 bytes
        return key >> (128 - (key_sizes[0] << 3));
    }

    ALWAYS_INLINE size_t windowKey(UInt256 key)
    {
        /// window time key is always: 2, 4 or 8 bytes
        return key >> (256 - (key_sizes[0] << 3));
    }

    ALWAYS_INLINE size_t windowKey(const StringRef & key)
    {
        /// deserialize the first key_sizes[0] bytes
        if (key_sizes[0] == 8)
        {
            assert(key.size > 8);
            return unalignedLoad<UInt64>(key.data);
        }
        else if (key_sizes[0] == 4)
        {
            assert(key.size > 4);
            return unalignedLoad<UInt32>(key.data);
        }
        else
        {
            assert(key.size > 2);
            assert(key_sizes[0] == 2);
            return unalignedLoad<UInt16>(key.data);
        }
    }

    ALWAYS_INLINE size_t windowKey(const DB::SerializedKeyHolder & key_holder) { return windowKey(key_holder.key); }

protected:
    typename Impl::iterator beginOfNextNonEmptyBucket(size_t & bucket)
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

    typename Impl::const_iterator beginOfNextNonEmptyBucket(size_t & bucket) const
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
    std::map<size_t, Impl> impls;
    Impl sentinel;

    StreamingTwoLevelHashTable() { }

    void setKeySizes(const std::vector<size_t> & key_sizes_)
    {
        assert(!key_sizes_.empty());
        assert(key_sizes_[0] == 2 || key_sizes_[0] == 4 || key_sizes_[0] == 8);

        key_sizes = key_sizes_;
    }

    /// Copy the data from another (normal) hash table. It should have the same hash function.
    template <typename Source>
    StreamingTwoLevelHashTable(const Source & src)
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
        size_t bucket{};
        typename Impl::iterator current_it{};

        friend class StreamingTwoLevelHashTable;

        iterator(Self * container_, size_t bucket_, typename Impl::iterator current_it_)
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
        size_t bucket{};
        typename Impl::const_iterator current_it{};

        friend class StreamingTwoLevelHashTable;

        const_iterator(Self * container_, size_t bucket_, typename Impl::const_iterator current_it_)
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
        size_t buck = 0;
        typename Impl::const_iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return {this, buck, impl_it};
    }

    iterator begin()
    {
        size_t buck = 0;
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
        auto window = windowKey(key_holder);
        impls[window].emplace(key_holder, it, inserted, hash_value);
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
            p.second.writeText(wb);
        }
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
            impls[key].readText(rb);
        }
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
            res += p.getBufferSizeInBytes();
        return res;
    }

    template <typename MappedDestroyFunc>
    std::pair<size_t, size_t> removeBucketsBeforeButKeep(UInt64 watermark, size_t num_bucket_to_keep, MappedDestroyFunc && mapped_destroy)
    {
        if (num_bucket_to_keep >= impls.size())
            return {0, impls.size()};

        size_t removed = 0;
        for (auto it = impls.begin(), it_end = impls.end(); it != it_end;)
        {
            if (it->first <= watermark)
            {
                it->second.forEachMapped(mapped_destroy);
                it->second.clearAndShrink();
                it = impls.erase(it);
                ++removed;

                if (impls.size() == num_bucket_to_keep)
                    break;
            }
            else
                break;
        }

        return {removed, impls.size()};
    }

    std::vector<size_t> bucketsBefore(UInt64 watermark)
    {
        std::vector<size_t> buckets;
        buckets.reserve(10);

        for (auto it = impls.begin(), it_end = impls.end(); it != it_end; ++it)
        {
            if (it->first <= watermark)
                buckets.push_back(it->first);
            else
                break;
        }

        return buckets;
    }
};
