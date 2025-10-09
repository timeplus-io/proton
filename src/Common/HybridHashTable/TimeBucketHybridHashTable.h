#pragma once

#include <Common/HybridHashTable/HybridHashTable.h>
#include <Common/PackedTimeBucket.h>

#include <absl/container/btree_map.h>

namespace DB
{
template <typename K>
class TimeBucketHybridHashTable
{
public:
    using KeySerializer = HybridHashTable<K>::KeySerializer;
    using KeyDeserializer = HybridHashTable<K>::KeyDeserializer;

    using KeyType = K;

    using Impl = HybridHashTable<K>;
    using ImplPtr = std::unique_ptr<Impl>;

    /// The first dimension is an integer 64 timestamp bucket
    /// Used by streaming window aggregation
    absl::btree_map<Int64, ImplPtr> bucket_tables;

public:
    TimeBucketHybridHashTable(
        HybridHashTableConfig config_, KeySerializer key_serializer_, KeyDeserializer key_deserializer_, LoggerPtr logger_)
        : config(config_), key_serializer(std::move(key_serializer_)), key_deserializer(std::move(key_deserializer_)), logger(logger_)
    {
    }

    const HybridHashTableConfig & getConfig() const noexcept { return config; }

    /// \param time_bucket_key_size_ the bytes of the time bucket,
    /// so far a 2, 4 or 8 bytes time bucket is supported
    void setBucketKeyBytesAndOffset(size_t time_bucket_key_size_, size_t time_bucket_key_offset_)
    {
        if (time_bucket_key_size_ == 2 || time_bucket_key_size_ == 4 || time_bucket_key_size_ == 8)
        {
            time_bucket_key_size = time_bucket_key_size_;
            time_bucket_key_offset = time_bucket_key_offset_;
        }
        else
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Time bucket key is expected to have 2, or 4 or 8 bytes, and it has {} bytes",
                time_bucket_key_size_);
        }
    }

    template <typename T>
    ALWAYS_INLINE Int64 timeBucket(T key)
    {
        return DB::decodeTimeBucket(key, time_bucket_key_size, time_bucket_key_offset);
    }

    ALWAYS_INLINE Int64 timeBucket(const std::string & key)
    {
        /// deserialize the first win_key_size bytes
        if (time_bucket_key_size == 8)
        {
            chassert(key.size() > 8);
            return unalignedLoad<Int64>(key.data());
        }
        else if (time_bucket_key_size == 4)
        {
            chassert(key.size() > 4);
            return unalignedLoad<UInt32>(key.data());
        }
        else if (time_bucket_key_size == 2)
        {
            chassert(key.size() > 2);
            return unalignedLoad<UInt16>(key.data());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "window key is expected to have 2, or 4 or 8 bytes");
        }
    }

    bool isTwoLevel() const noexcept { return true; }

    HybridEmplaceResult emplaceKey(const K & key, bool disable_spill) { return emplaceKey(key, /*is_new_key=*/false, disable_spill); }

    HybridEmplaceResult emplaceNewKey(const K & key) { return emplaceKey(key, /*is_new_key=*/true, /*disable_spill=*/false); }

    /// Call shall call `spillIfNecessary()` after done with the values
    HybridEmplaceResults emplaceKeys(const std::vector<K> & keys) { return emplaceKeys(keys, /*is_new_keys=*/false); }

    /// Call shall call `spillIfNecessary()` after done with the values
    HybridEmplaceResults emplaceNewKeys(const std::vector<K> & keys) { return emplaceKeys(keys, /*is_new_keys=*/true); }

    HybridFindResult findKey(const K & key, bool disable_spill)
    {
        auto bucket = timeBucket(key);
        if (auto iter = bucket_tables.find(bucket); iter != bucket_tables.end())
            return iter->second->findKey(key, disable_spill);
        else
            return HybridFindResult{};
    }

    HybridFindResult findKeyWithoutSpill(const K & key)
    {
        auto bucket = timeBucket(key);
        if (auto iter = bucket_tables.find(bucket); iter != bucket_tables.end())
            return iter->second->findKeyWithoutSpill(key);
        else
            return HybridFindResult{};
    }

    /// Call shall call `spillIfNecessary()`
    HybridFindResults findKeys(const std::vector<K> & keys)
    {
        chassert(!keys.empty());

        HybridFindResults results;

        size_t current_idx = 0;
        Int64 current_bucket = timeBucket(keys.front());

        auto find_one_bucket = [&, this](size_t n) {
            /// bucket changes, find keys in the current batch in range [current_idx, i) first
            auto iter = bucket_tables.find(current_bucket);
            if (iter != bucket_tables.end())
            {
                auto batch_results = iter->second->findKeys(keys.begin() + current_idx, keys.begin() + n, /*disable_spill=*/true);

                if (!batch_results.hasError())
                {
                    if (!results.results.empty())
                    {
                        for (auto & result : batch_results.results)
                            results.results.push_back(std::move(result));
                    }
                    else
                    {
                        results.results.swap(batch_results.results);
                        results.results.reserve(keys.size());
                    }
                }
                return batch_results.errcode;
            }
            else
            {
                /// Bucket doesn't exist, insert empty find results
                for (size_t i = current_idx; i < n; ++i)
                    results.results.emplace_back();

                return ErrorCodes::OK;
            }
        };

        for (size_t i = 1, key_size = keys.size(); i < key_size; ++i)
        {
            auto bucket = timeBucket(keys[i]);
            if (bucket != current_bucket)
            {
                if (auto errcode = find_one_bucket(i); errcode != ErrorCodes::OK)
                    return HybridFindResults{errcode};

                /// Update current bucket and progress current index
                current_bucket = bucket;
                current_idx = i;
            }
        }

        /// Last batch
        if (auto errcode = find_one_bucket(current_idx); errcode != ErrorCodes::OK)
            return HybridFindResults{errcode};

        return results;
    }

    int forEachKey(std::function<void(const K &)> callback) const
    {
        for (const auto & [_, impl] : bucket_tables)
        {
            if (auto errcode = impl->forEachKey(callback); errcode != ErrorCodes::OK)
                return errcode;
        }

        return ErrorCodes::OK;
    }

    int forEachKeyValue(std::function<void(const K &, HybridMappedValue)> callback) const
    {
        for (const auto & [_, impl] : bucket_tables)
        {
            if (auto errcode = impl->forEachKeyValue(callback); errcode != ErrorCodes::OK)
                return errcode;
        }

        return ErrorCodes::OK;
    }

    int forBatchValue(
        size_t batch_size,
        std::function<void(const K &, HybridMappedValue, bool /*flush*/)> callback,
        std::function<void()> done_callback) const
    {
        for (const auto & [_, impl] : bucket_tables)
        {
            if (auto errcode = impl->forBatchValue(batch_size, callback, done_callback); errcode != ErrorCodes::OK)
                return errcode;
        }

        return ErrorCodes::OK;
    }

    int spillIfNecessary() { return spillIfNecessary(config.max_hot_key_count); }

    int spillIfNecessary(size_t current_batch_size)
    {
        for (const auto & [_, impl] : bucket_tables)
        {
            if (auto errcode = impl->spillIfNecessary(current_batch_size); errcode != ErrorCodes::OK)
                return errcode;
        }

        return ErrorCodes::OK;
    }

    void flush()
    {
        for (const auto & [_, impl] : bucket_tables)
            impl->flush();
    }

    void reload(const HybridHashTableConfig::ValueDeserializer & old_value_deserializer = {})
    {
        for (const auto & [_, impl] : bucket_tables)
            impl->reload(old_value_deserializer);
    }

    std::unique_ptr<Impl> newUnsharedBucketTable(std::string_view id)
    {
        return std::make_unique<Impl>(config.getSubConfig(id, /*unshared=*/true), key_serializer, key_deserializer, logger);
    }

    const HybridHashTableMetrics & getMetrics() const noexcept
    {
        metrics = HybridHashTableMetrics{};
        for (const auto & [_, impl] : bucket_tables)
            metrics += impl->getMetrics();

        return metrics;
    }

    size_t approximateCount() const
    {
        size_t estimated_keys = 0;
        for (const auto & [_, impl] : bucket_tables)
            estimated_keys += impl->approximateCount();

        return estimated_keys;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t total_buffer_bytes = 0;
        for (const auto & [_, impl] : bucket_tables)
            total_buffer_bytes += impl->getBufferSizeInBytes();

        return total_buffer_bytes;
    }

    size_t getBufferSizeInCells() const
    {
        size_t total_buffer_cells = 0;
        for (const auto & [_, impl] : bucket_tables)
            total_buffer_cells += impl->getBufferSizeInCells();

        return total_buffer_cells;
    }

    /// Keep the latest X windows. If there is a gap in between, we still need clean the old window if they are X * interval
    /// after the current watermark
    /// Return {removed, last_removed_watermark, remaining_size}
    std::tuple<size_t, Int64, size_t> removeBucketsBefore(Int64 max_bucket)
    {
        Int64 last_removed_watermark = 0;
        size_t removed = 0;

        for (auto it = bucket_tables.begin(); it != bucket_tables.end();)
        {
            if (it->first <= max_bucket)
            {
                last_removed_watermark = it->first;
                ++removed;

                it->second->destroyPersistentPart();
                it = bucket_tables.erase(it);
            }
            else
            {
                break;
            }
        }

        auto new_size = bucket_tables.size();
        return {removed, last_removed_watermark, new_size};
    }

    std::vector<Int64> bucketsBefore(Int64 max_bucket) const
    {
        std::vector<Int64> buckets;
        buckets.reserve(10);

        for (const auto & [bucket, impl] : bucket_tables)
        {
            if (bucket <= max_bucket)
                buckets.push_back(bucket);
            else
                break;
        }

        return buckets;
    }

    std::vector<Int64> buckets() const
    {
        std::vector<Int64> buckets;
        buckets.reserve(bucket_tables.size());

        for (const auto & [bucket, _] : bucket_tables)
            buckets.push_back(bucket);

        return buckets;
    }

    void reinitBuckets(const std::vector<Int64> & buckets)
    {
        clear();
        for (auto bucket : buckets)
        {
            HybridHashTableConfig new_config = config.getSubConfig(fmt::format("bucket-{}", bucket));
            auto impl = std::make_unique<Impl>(std::move(new_config), key_serializer, key_deserializer, logger);
            [[maybe_unused]] auto [_, inserted] = bucket_tables.emplace(bucket, std::move(impl));
            chassert(inserted);
        }
    }

    std::map<Int64, Impl *> bucketTables(const std::vector<Int64> & buckets) const
    {
        std::map<Int64, Impl *> result;

        for (auto bucket : buckets)
        {
            if (auto iter = bucket_tables.find(bucket); iter != bucket_tables.end())
                result.emplace(iter->first, iter->second.get());
        }

        return result;
    }

    bool removeBucketTable(Int64 bucket) { return bucket_tables.erase(bucket) == 1; }

    Impl * bucketTable(Int64 bucket)
    {
        if (auto iter = bucket_tables.find(bucket); iter != bucket_tables.end())
            return iter->second.get();

        return nullptr;
    }

    bool hasBucket(Int64 bucket) const noexcept { return bucket_tables.contains(bucket); }

    void clear()
    {
        bucket_tables.clear();
        metrics = HybridHashTableMetrics{};
    }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(bucket_tables.size(), wb);
        for (const auto & [bucket, impl] : bucket_tables)
        {
            writeVarInt(bucket, wb);
            impl->write(wb);
        }
    }

    void read(ReadBuffer & rb, const HybridHashTableConfig::ValueDeserializer & old_value_deserializer = {})
    {
        chassert(bucket_tables.empty());

        size_t num_buckets = 0;
        readVarUInt(num_buckets, rb);

        for (size_t i = 0; i < num_buckets; ++i)
        {
            Int64 bucket = 0;
            readVarInt(bucket, rb);

            HybridHashTableConfig new_config = config.getSubConfig(fmt::format("bucket-{}", bucket));
            auto impl = std::make_unique<Impl>(std::move(new_config), key_serializer, key_deserializer, logger);
            impl->read(rb, old_value_deserializer);

            [[maybe_unused]] auto [_, inserted] = bucket_tables.emplace(bucket, std::move(impl));
            chassert(inserted);
        }
    }

    int removeKey(const K & key)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Remove key from time bucket hybrid hash table is not supported yet");
    }

private:
    HybridEmplaceResult emplaceKey(const K & key, bool is_new_key, bool disable_spill)
    {
        auto bucket = timeBucket(key);
        Impl * table = nullptr;
        if (auto iter = bucket_tables.find(bucket); iter != bucket_tables.end())
        {
            table = iter->second.get();
        }
        else
        {
            HybridHashTableConfig new_config = config.getSubConfig(fmt::format("bucket-{}", bucket));
            auto impl = std::make_unique<Impl>(std::move(new_config), key_serializer, key_deserializer, logger);
            table = impl.get();
            [[maybe_unused]] auto [_, inserted] = bucket_tables.emplace(bucket, std::move(impl));
            chassert(inserted);
        }
        return is_new_key ? table->emplaceNewKey(key) : table->emplaceKey(key, disable_spill);
    }

    HybridEmplaceResults emplaceKeys(const std::vector<K> & keys, bool is_new_keys)
    {
        chassert(!keys.empty());

        HybridEmplaceResults results;

        size_t current_idx = 0;
        Int64 current_bucket = timeBucket(keys.front());

        auto emplace_one_bucket = [&, this](size_t n) {
            /// bucket changes, finish the current batch in range [current_idx, i) first
            Impl * table = nullptr;
            auto iter = bucket_tables.find(current_bucket);
            if (iter != bucket_tables.end())
            {
                table = iter->second.get();
            }
            else
            {
                HybridHashTableConfig new_config = config.getSubConfig(fmt::format("bucket-{}", current_bucket));
                auto impl = std::make_unique<Impl>(std::move(new_config), key_serializer, key_deserializer, logger);
                table = impl.get();
                [[maybe_unused]] auto [_, inserted] = bucket_tables.emplace(current_bucket, std::move(impl));
                chassert(inserted);
            }

            /// Insert current batch keys and merge insert results
            auto batch_results = is_new_keys ? table->emplaceNewKeys(keys.begin() + current_idx, keys.begin() + n, /*disable_spill=*/true)
                                             : table->emplaceKeys(keys.begin() + current_idx, keys.begin() + n, /*disable_spill=*/true);

            if (!batch_results.hasError())
            {
                if (!results.results.empty())
                {
                    for (auto & result : batch_results.results)
                        results.results.push_back(std::move(result));
                }
                else
                {
                    results.results.swap(batch_results.results);
                    results.results.reserve(keys.size());
                }
            }

            return batch_results.errcode;
        };

        size_t i = 1;
        for (auto key_size = keys.size(); i < key_size; ++i)
        {
            auto bucket = timeBucket(keys[i]);
            if (bucket != current_bucket)
            {
                if (auto errcode = emplace_one_bucket(i); errcode != ErrorCodes::OK)
                    return HybridEmplaceResults{errcode};

                /// Update current bucket and progress current index
                current_bucket = bucket;
                current_idx = i;
            }
        }

        /// Last batch
        if (auto errcode = emplace_one_bucket(i); errcode != ErrorCodes::OK)
            return HybridEmplaceResults{errcode};

        return results;
    }

private:
    HybridHashTableConfig config;
    KeySerializer key_serializer;
    KeyDeserializer key_deserializer;

    mutable HybridHashTableMetrics metrics;

    /// The size of the time bucket key, either 2 or 4 or 8 bytes
    /// Time bucket key is used to lookup the time \bucket_tables
    size_t time_bucket_key_size = 0;
    /// The offset of the time bucket key encoded in the lookup key
    /// Used only when there is nullable group by keys which introduces padding
    size_t time_bucket_key_offset = 0;

    LoggerPtr logger;
};

}
