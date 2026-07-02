#pragma once

#include <Cluster/Common/LogTrack.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/HybridHashTable/HybridMappedValue.h>
#include <Common/MemoryHelpers.h>
/// #include <Common/MemoryTrackerBlockerInThread.h>
#include <base/ClockUtils.h>
#include <Common/HybridConfig.h>
#include <Common/Rocks/RocksDB.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/utilities/memory_util.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_OPEN_DATABASE;
extern const int INVALID_CONFIG_PARAMETER;
extern const int NOT_IMPLEMENTED;
extern const int ROCKSDB_ERROR;
extern const int OK;
extern const int BAD_VERSION;
}

/// A hybrid hash table which is optimized for large scale key aggregation / caching
/// when there are lots of keys, spill to disk.
struct HybridHashTableConfig
{
    void validate()
    {
        base_conf.validate();

        if (!value_constructor)
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: value constructor is not setup");

        if (!value_destructor)
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: value destructor is not setup");

        if (!value_serializer)
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: value serializer is not setup");

        if (!value_deserializer)
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: value deserializer is not setup");
    }

    void installNoOpCallbacks()
    {
        /// Please note when value_object_size is zero, std::malloc(0) still returns a valid address,
        /// we actually need this unique address for tracking etc purposes
        value_object_size = 0;
        value_constructor = ([](void * /*data*/) { });
        value_destructor = ([](void * /*data*/) { });
        value_serializer = ([](const void * /*data*/, WriteBuffer &) { return DB::ErrorCodes::OK; });
        value_deserializer = ([](void * /*data*/, ReadBuffer &) { return DB::ErrorCodes::OK; });
    }

    rocksdb::Options getRocksOptions() const { return base_conf.getRocksOptions(); }

    HybridHashTableConfig getSubConfig(std::string_view sub_id, bool unshared = false) const
    {
        HybridHashTableConfig new_config = *this;
        new_config.base_conf = base_conf.getSubConfig(sub_id, unshared);
        return new_config;
    }

    HybridConfig base_conf;

    /// Value object size is used to allocate enough memory to hold the object
    size_t value_object_size = 0;
    size_t align_value_object_size = sizeof(void *);

    /// Value can contain a list of columns, each of the column can have its own serialization
    /// and deserialization methods. They are called when spilling to persistent storage or
    /// loading from persistent storage to memory.
    ///
    /// ValueSerializer serializes from `const void *` which can point to a complex data structure
    /// (for example aggregation) to write buffer.
    /// ValueDeserializer deserializes from read buffer to the `void *` to construct target value
    /// which may be complex data structure.
    using ValueConstructor = std::function<void(void *)>;
    using ValueDestructor = std::function<void(void *)>;

    using ValueSerializer = std::function<int(const void *, WriteBuffer &)>;
    using ValueDeserializer = std::function<int(void *, ReadBuffer &)>;

    ValueConstructor value_constructor;
    ValueDestructor value_destructor; /// Shall never raise exception, otherwise there may be memory leak
    ValueSerializer value_serializer;
    ValueDeserializer value_deserializer;
};

struct HybridHashTableMetrics
{
    uint64_t read_total = 0;
    uint64_t read_persistent = 0;
    uint64_t write_total = 0;
    uint64_t write_new = 0;
    uint64_t spilled = 0;

    std::string string() const
    {
        return fmt::format(
            "read_total={} read_persistent={} write_total={} write_new={} spilled={}",
            read_total,
            read_persistent,
            write_total,
            write_new,
            spilled);
    }

    HybridHashTableMetrics & operator+=(const HybridHashTableMetrics & rhs)
    {
        read_total += rhs.read_total;
        read_persistent += rhs.read_persistent;
        write_total += rhs.write_total;
        write_new += rhs.write_new;
        spilled += rhs.spilled;

        return *this;
    }

    void serialize(WriteBuffer & wb, VersionType /*version*/) const
    {
        DB::writeVarUInt(read_total, wb);
        DB::writeVarUInt(read_persistent, wb);
        DB::writeVarUInt(write_total, wb);
        DB::writeVarUInt(write_new, wb);
        DB::writeVarUInt(spilled, wb);
    }

    void deserialize(ReadBuffer & rb, VersionType /*version*/)
    {
        DB::readVarUInt(read_total, rb);
        DB::readVarUInt(read_persistent, rb);
        DB::readVarUInt(write_total, rb);
        DB::readVarUInt(write_new, rb);
        DB::readVarUInt(spilled, rb);
    }
};

struct HybridFindResult
{
    HybridFindResult() = default;
    explicit HybridFindResult(int errcode_) : errcode(errcode_) { }
    explicit HybridFindResult(HybridMappedValue value_) : value(value_) { }

    ALWAYS_INLINE const void * getMapped() const noexcept { return value.getMapped(); }
    ALWAYS_INLINE void * getMutableMapped() noexcept { return value.getMutableMapped(); }

    bool hasError() const noexcept { return errcode != ErrorCodes::OK; }
    std::string errorString() const noexcept { return fmt::format("errcode={} message={}", errcode, ErrorCodes::getName(errcode)); }
    bool isFound() const noexcept { return value.isValid(); }
    bool isNotFound() const noexcept { return !value.isValid(); }

    HybridMappedValue value;
    int errcode = ErrorCodes::OK;
};

struct HybridFindResults
{
    HybridFindResults() = default;
    explicit HybridFindResults(int errcode_) : errcode(errcode_) { }
    bool hasError() const noexcept { return errcode != ErrorCodes::OK; }
    int errorCode() const noexcept { return errcode; }
    std::string errorString() const noexcept { return fmt::format("errcode={} message={}", errcode, ErrorCodes::getName(errcode)); }

    std::vector<HybridFindResult> results;
    int errcode = ErrorCodes::OK;
};

struct HybridEmplaceResult
{
    HybridEmplaceResult() = default;
    explicit HybridEmplaceResult(int errcode_) : find_result(errcode_) { }
    HybridEmplaceResult(HybridFindResult && find_result_, bool inserted_)
        : find_result(std::move(find_result_)), inserted(inserted_), inserted_in_batch(inserted_)
    {
    }
    HybridEmplaceResult(HybridMappedValue value_, bool inserted_)
        : find_result(value_), inserted(inserted_), inserted_in_batch(inserted) { }
    HybridEmplaceResult(HybridMappedValue value_, bool inserted_, bool inserted_in_batch_)
        : find_result(value_), inserted(inserted_), inserted_in_batch(inserted_in_batch_)
    {
        /// inserted = true, inserted_in_batch = true
        /// inserted = false, inserted_in_batch = true
        /// inserted = false, inserted_in_batch = false
        /// inserted = true, inserted_in_batch = false /// In valid state
        chassert(inserted_in_batch || (!inserted_in_batch && !inserted));
    }
    bool isInserted() const noexcept { return inserted; }
    bool isInsertedInBatch() const noexcept { return inserted_in_batch; }

    ALWAYS_INLINE const void * getMapped() const noexcept { return find_result.getMapped(); }
    ALWAYS_INLINE void * getMutableMapped() noexcept { return find_result.getMutableMapped(); }

    bool hasError() const noexcept { return find_result.hasError(); }
    int errorCode() const noexcept { return find_result.errcode; }
    std::string errorString() const noexcept { return find_result.errorString(); }

    HybridFindResult find_result;
    bool inserted = false;
    /// When using batch API to emplaceKeys, there may be duplicate new keys in the parameter
    /// The first new key will be inserted and the second key will be found in the hot cache
    /// hence treated not inserted (found) but inserted in batch
    /// Example, k1, k2, k1, k3
    /// The first `k1` is newly inserted, and the following `k1` is NOT treated as inserted (inserted = false)
    /// but treated as inserted in batch (inserted_in_batch = true)
    bool inserted_in_batch = false;
};

struct HybridEmplaceResults
{
    HybridEmplaceResults() = default;
    explicit HybridEmplaceResults(int errcode_) : errcode(errcode_) { }
    bool hasError() const noexcept { return errcode != ErrorCodes::OK; }
    int errorCode() const noexcept { return errcode; }
    std::string errorString() const noexcept { return fmt::format("errcode={} message={}", errcode, ErrorCodes::getName(errcode)); }

    std::vector<HybridEmplaceResult> results;
    int errcode = ErrorCodes::OK;
};

template <typename K, typename Hash = absl::DefaultHashContainerHash<K>>
class HybridHashTable
{
public:
    using Self = HybridHashTable;

    using KeySerializer = std::function<int(const K &, WriteBuffer &)>;
    using KeyDeserializer = std::function<int(K &, ReadBuffer &)>;

    using KeyType = K;

    using Mapped = void *;

    struct Entry : HybridMappedValue
    {
        using HybridMappedValue::HybridMappedValue;

        friend class HybridHashTable;

        std::list<K>::iterator recent_keys_position;
    };

    using InmemoryHashMap = absl::flat_hash_map<K, Entry, Hash>;

public:
    HybridHashTable(HybridHashTableConfig config_, KeySerializer key_serializer_, KeyDeserializer key_deserializer_, LoggerPtr logger_)
        : config(std::move(config_))
        , key_serializer(std::move(key_serializer_))
        , key_deserializer(std::move(key_deserializer_))
        , logger(logger_)
    {
        chassert(key_serializer && key_deserializer);
        if (std::filesystem::exists(config.base_conf.spill_dir_path) && !config.base_conf.rocks_cf_handler_getter)
            /// If unshared rocks directory already exists, init rocks eagerly, otherwise in a lazy way
            reload();
    }

    ~HybridHashTable()
    {
        try
        {
            clear();
        }
        catch (...)
        {
            tryLogCurrentException(logger, "Exception happened in ~HybridHashTable");
        }
    }

    const HybridHashTableConfig & getConfig() const noexcept { return config; }

    RocksDBPtr getRocksDB() const
    {
        if (persistentPartInited())
            return cf_handler->getRocksDB();
        else
            return nullptr;
    }

    void destroyPersistentPart()
    {
        if (!persistentPartInited())
            return;

        /// For shared rocks, we just destroy current handler and column family
        if (!rocks && cf_handler && config.base_conf.cleanup_on_disk_data)
            cf_handler->destroy();

        cf_handler.reset();
        rocks.reset();
    }

    bool isTwoLevel() const noexcept { return false; }

    void clear()
    {
        if (!config.base_conf.cleanup_on_disk_data)
        {
            /// If we like to retain the data around, flush them to disk
            bulkSpill(recent_keys.size());
        }
        else
        {
            removeNHotKeyValues(recent_keys.size());
            destroyPersistentPart();
        }

        hot_key_values.clear();
        recent_keys.clear();
        full_cached = true;

        metrics = HybridHashTableMetrics{};

        cf_handler.reset();
        rocks.reset();
    }

    HybridEmplaceResult emplaceKey(const K & key, bool disable_spill)
    {
        auto find_result = doFindKey(key);
        if (find_result.isFound())
        {
            if (!disable_spill)
                spillIfNecessary(/*current_batch_size=*/1);
            return HybridEmplaceResult{std::move(find_result), /*inserted_=*/false};
        }

        if (find_result.hasError())
            return HybridEmplaceResult{std::move(find_result), /*inserted_=*/false};

        chassert(recent_keys.size() == hot_key_values.size());

        return emplaceNewKey(key, disable_spill);
    }

    HybridEmplaceResult emplaceNewKey(const K & key, bool disable_spill)
    {
        ++metrics.write_total;
        ++metrics.write_new;

        auto & entry = insert(key, constructValue());

        if (!disable_spill)
            spillIfNecessary(/*current_batch_size=*/1);

        chassert(recent_keys.size() == hot_key_values.size());

        return HybridEmplaceResult{entry, /*inserted_=*/true};
    }

    HybridEmplaceResults emplaceKeys(const std::vector<K> & keys) { return emplaceKeys(keys.begin(), keys.end(), /*disable_spill=*/false); }

    /// \param disable_spill if it is true, max hot keys threshold won't be checked and spill to disk is
    /// guaranteed not going to happen during this batch call. Use this parameter carefully since it may
    /// cause too many keys loaded into memory. Usually clients call spillIfNecessary() manually when
    /// disable_spill is true.
    /// For example,
    /// htb.emplaceKeys(keys, true);
    /// htb.emplaceKeys(keys, true);
    /// htb.emplaceKeys(keys, true);
    /// htb.spillIfNecessary()
    HybridEmplaceResults emplaceKeys(std::vector<K>::const_iterator keys_start, std::vector<K>::const_iterator keys_end, bool disable_spill)
    {
        size_t key_count = std::distance(keys_start, keys_end);
        metrics.write_total += key_count;

        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Inserted {} keys, took {}ms", key_count, elapsed_ms);
        });

        auto find_results = doFindKeys(keys_start, keys_end);
        if (find_results.hasError())
            return HybridEmplaceResults{find_results.errcode};

        chassert(find_results.results.size() == key_count);

        HybridEmplaceResults results;
        results.results.reserve(key_count);

        auto keys_iter = keys_start;
        for (size_t i = 0; keys_iter != keys_end; ++keys_iter, ++i)
        {
            auto & find_result = find_results.results[i];

            chassert(!find_result.hasError());
            if (find_result.isFound())
            {
                results.results.emplace_back(std::move(find_result), /*inserted_=*/false);
                continue;
            }

            /// New key
            ++metrics.write_new;

            /// There may be duplicated keys in \keys passed in which have been loaded / inserted already
            if (auto iter = hot_key_values.find(*keys_iter); iter != hot_key_values.end())
            {
                /// Duplicated new key is found in the current batch, treat it as NOT inserted but inserted in batch
                results.results.emplace_back(iter->second, /*inserted_=*/false, /*inserted_in_batch_=*/true);
                continue;
            }

            auto & entry = insert(*keys_iter, constructValue());
            results.results.emplace_back(entry, /*inserted_=*/true);
        }

        if (!disable_spill)
            spillIfNecessary(/*current_batch_size=*/key_count);

        chassert(recent_keys.size() == hot_key_values.size());
        return results;
    }

    /// All keys are new, it is an optimization (during backfill for example)
    /// The code will skip looking up the keys first and go ahead to insert the keys.
    /// Be careful when calling this method since if keys are not new, the hashtable
    /// may have incorrect behaviors
    /// \params keys new keys which don't have duplicates
    /// FIXME, it is interesting doing this forward call with `std::span{keys}` introduces almost 100% perf degradation on Mac Pro M2 Max
    HybridEmplaceResults emplaceNewKeys(const std::vector<K> & keys)
    {
        return emplaceNewKeys(keys.begin(), keys.end(), /*disable_spill=*/false);
    }

    /// \param disable_spill Same as in emplaceKeys(..., disable_spill)
    HybridEmplaceResults
    emplaceNewKeys(std::vector<K>::const_iterator keys_start, std::vector<K>::const_iterator keys_end, bool disable_spill)
    {
        auto key_count = std::distance(keys_start, keys_end);

        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Inserted {} new keys, took {}ms", key_count, elapsed_ms);
        });

        metrics.write_total += key_count;
        ++metrics.write_new += key_count;

        HybridEmplaceResults results;
        results.results.reserve(key_count);

        for (; keys_start != keys_end; ++keys_start)
        {
            auto & entry = insert(*keys_start, constructValue());
            results.results.emplace_back(entry, /*inserted_=*/true);
        }

        if (!disable_spill)
            spillIfNecessary(/*current_batch_size=*/key_count);

        chassert(recent_keys.size() == hot_key_values.size());
        return results;
    }

    HybridFindResult findKey(const K & key, bool disable_spill)
    {
        auto find_result = doFindKey(key);
        if (find_result.isFound() && !disable_spill)
            spillIfNecessary(/*current_batch_size=*/1);

        chassert(recent_keys.size() == hot_key_values.size());
        return find_result;
    }

    HybridFindResults findKeys(const std::vector<K> & keys) { return findKeys(keys.begin(), keys.end(), /*disable_spill=*/false); }

    HybridFindResults findKeys(const std::vector<K> & keys, bool disable_spill)
    {
        return findKeys(keys.begin(), keys.end(), disable_spill);
    }

    /// \param disable_spill Same in emplaceKeys(keys, disable_spill)
    HybridFindResults findKeys(std::vector<K>::const_iterator keys_start, std::vector<K>::const_iterator keys_end, bool disable_spill)
    {
        auto key_count = std::distance(keys_start, keys_end);
        auto find_results = doFindKeys(keys_start, keys_end);
        if (!disable_spill)
            spillIfNecessary(key_count);

        chassert(recent_keys.size() == hot_key_values.size());
        return find_results;
    }

    /// \return true if key already exists, otherwise false
    /// This may be expensive if it hits on disk lookup
    bool contains(const K & key)
    {
        auto find_result = doFindKey(key);
        if (find_result.isFound())
            spillIfNecessary(/*current_batch_size=*/1);

        chassert(recent_keys.size() == hot_key_values.size());
        return find_result.isFound();
    }

    /// Iterate each key and call callback function
    /// The life cycle of the key is only valid during the callback is invoked, a.k.a the callback
    /// shall never save a reference to key for later use
    int forEachKey(std::function<void(const K &)> callback) const
    {
        chassert(callback);

        size_t key_count = 0;
        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Looping total {} keys, took {}ms", key_count, elapsed_ms);
        });

        key_count += hot_key_values.size();

        /// Serve hot keys first
        for (const auto & [key, _] : hot_key_values)
            callback(key);

        if (persistentPartInited() && !full_cached)
        {
            auto scan_options = read_options;
            scan_options.fill_cache = false;
            std::unique_ptr<rocksdb::Iterator> iter{cf_handler->db->NewIterator(scan_options, cf_handler->cf_handle)};
            for (iter->SeekToFirst(); iter->Valid(); iter->Next())
            {
                K k;
                {
                    ReadBufferFromMemory rb{iter->key().data(), iter->key().size()};
                    if (auto errcode = key_deserializer(k, rb); errcode != ErrorCodes::OK)
                        return errcode;
                }

                if (hot_key_values.contains(k))
                    /// Already served
                    continue;

                ++key_count;
                callback(k);
            }
        }

        return ErrorCodes::OK;
    }

    /// Iterate each key / value pair and call callback function
    /// The life cycle of the key and value is only valid during the callback is invoked, a.k.a the callback
    /// shall never save a reference to key and value for later use
    int forEachKeyValue(std::function<void(const K &, HybridMappedValue)> callback)
    {
        chassert(callback);

        size_t key_count = 0;
        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Looping total {} keys, took {}ms", key_count, elapsed_ms);
        });

        key_count += hot_key_values.size();

        /// Serve hot keys first
        for (auto & [key, entry] : hot_key_values)
            callback(key, entry);

        if (persistentPartInited() && !full_cached)
        {
            HybridMappedValue::ControlBits control{.loaded = true};
            HybridMappedValue mapped_value(nullptr, control);

            auto scan_options = read_options;
            scan_options.fill_cache = false;
            std::unique_ptr<rocksdb::Iterator> iter{cf_handler->db->NewIterator(scan_options, cf_handler->cf_handle)};
            for (iter->SeekToFirst(); iter->Valid(); iter->Next())
            {
                K k;
                {
                    ReadBufferFromMemory rb{iter->key().data(), iter->key().size()};
                    if (auto errcode = key_deserializer(k, rb); errcode != ErrorCodes::OK)
                        return errcode;
                }

                if (hot_key_values.contains(k))
                    /// Already served
                    continue;

                ++key_count;
                {
                    auto value_ptr = constructValue();
                    {
                        ReadBufferFromMemory rb{iter->value().data(), iter->value().size()};
                        if (auto errcode = config.value_deserializer(value_ptr.get(), rb); errcode != ErrorCodes::OK)
                            return errcode;
                    }

                    mapped_value.setData(value_ptr.get());
                    callback(k, mapped_value);
                }
            }
        }

        return ErrorCodes::OK;
    }

    /// Iterate each key / value pair and call callback function
    /// This is a special version of forEachKeyValue since the lift cycle of the key passed to the callback is
    /// the same as forEachKeyValue, which means key is only valid when callback is invoked.
    /// The lift cycle of value is more tricky since
    /// 1) forBatchValue holds on to the value (pointers) in a batch until a `flush = true` is passed to the callback
    /// 2) In the callback, it can save the value points for batch processing and shall not reference them any more
    ///    after `flush=true` is passed in (which means clients need process the batch so far and clear the batch)
    /// \done_callback when done with all keys and before release the last batch values memory, invoke it
    /// \return DB::ErrorCodes::OK if success, otherwise return corresponding error code if failed in the middle
    int forBatchValue(
        size_t batch_size, std::function<void(const K &, HybridMappedValue, bool /*flush*/)> callback, std::function<void()> done_callback)
    {
        chassert(callback && done_callback);

        size_t key_count = 0;
        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Looping total {} keys, took {}ms", key_count, elapsed_ms);
        });

        key_count += hot_key_values.size();
        /// Serve hot keys first
        for (auto & [key, entry] : hot_key_values)
            callback(key, entry, /*flush=*/false);

        if (persistentPartInited() && !full_cached)
        {
            HybridMappedValue::ControlBits control{.loaded = true};
            HybridMappedValue mapped_value(nullptr, control);

            std::vector<decltype(constructValue())> value_batch;
            value_batch.reserve(batch_size);

            auto scan_options = read_options;
            scan_options.fill_cache = false;
            std::unique_ptr<rocksdb::Iterator> iter{cf_handler->db->NewIterator(scan_options, cf_handler->cf_handle)};
            for (iter->SeekToFirst(); iter->Valid(); iter->Next())
            {
                K k;
                {
                    ReadBufferFromMemory rb{iter->key().data(), iter->key().size()};
                    if (auto errcode = key_deserializer(k, rb); errcode != ErrorCodes::OK)
                        return errcode;
                }

                if (hot_key_values.contains(k))
                    /// Already served
                    continue;

                ++key_count;
                {
                    value_batch.push_back(constructValue()); /// Hold on to the value in the batch
                    const auto & value_ptr = value_batch.back();
                    {
                        ReadBufferFromMemory rb{iter->value().data(), iter->value().size()};
                        if (auto errcode = config.value_deserializer(value_ptr.get(), rb); errcode != ErrorCodes::OK)
                            return errcode;
                    }

                    bool flush = value_batch.size() >= batch_size;

                    mapped_value.setData(value_ptr.get());
                    callback(k, mapped_value, flush);

                    if (flush)
                        value_batch.clear();
                }
            }

            done_callback();
        }
        else
        {
            /// All keys are hot and in memory
            done_callback();
        }

        return ErrorCodes::OK;
    }

    /// \return DB::ErrorCodes::OK if no failure, otherwise return corresponding error code
    int removeKey(const K & key)
    {
        if (persistentPartInited())
        {
            /// Delete from persistent key value store
            std::vector<char> key_data;
            {
                WriteBufferFromVector<std::vector<char>> wb(key_data);
                if (auto errcode = key_serializer(key, wb); errcode != ErrorCodes::OK)
                    return errcode;
            }

            if (auto status = cf_handler->db->Delete(write_options, cf_handler->cf_handle, {key_data.data(), key_data.size()});
                !status.ok())
            {
                LOG_ERROR(logger, "Failed to delete key on disk, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }

        removeOneHotKey(key);

        chassert(recent_keys.size() == hot_key_values.size());

        return ErrorCodes::OK;
    }

    /// \return DB::ErrorCodes::OK if no failure, otherwise return corresponding error code
    int removeKeys(const std::vector<K> & keys)
    {
        if (persistentPartInited())
        {
            /// Delete from persistent key / value store
            std::vector<std::vector<char>> keys_data;
            keys_data.reserve(keys.size());

            size_t approx_keys_size = 0;

            for (const auto & key : keys)
            {
                std::vector<char> key_data;
                {
                    WriteBufferFromVector<std::vector<char>> wb(key_data);
                    if (auto errcode = key_serializer(key, wb); errcode != ErrorCodes::OK)
                        return errcode;
                }

                approx_keys_size += key_data.size();
                keys_data.push_back(std::move(key_data));
            }

            rocksdb::WriteBatch batch{static_cast<size_t>(approx_keys_size * 1.5)};
            for (const auto & key_data : keys_data)
            {
                auto status = batch.Delete(cf_handler->cf_handle, {key_data.data(), key_data.size()});
                if (!status.ok())
                {
                    LOG_ERROR(logger, "Failed to add key/value to batch, status='{}'", status.ToString());
                    return ErrorCodes::ROCKSDB_ERROR;
                }
            }

            if (auto status = cf_handler->db->Write(write_options, &batch); !status.ok())
            {
                LOG_ERROR(logger, "Failed to spill key/values to disk, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }

        for (const auto & key : keys)
            removeOneHotKey(key);

        chassert(recent_keys.size() == hot_key_values.size());
        return ErrorCodes::OK;
    }

    static constexpr VersionType version = 2;

    void write(WriteBuffer & wb) const
    {
        /// Layout: version, cleanup_on_disk_data, batch_size1, key1, value1, key2, value2, batch_size2, ..., empty_batch_size(end), metrics
        writeBinary(version, wb);

        writeBinary<bool>(config.base_conf.cleanup_on_disk_data, wb);

        if (persistentPartInited() && !full_cached)
        {
            /// Flush all hot keys to disk
            const_cast<Self *>(this)->flush();

            /// loop each serialized key / value from cf_handler->db
            String batch_data;
            WriteBufferFromString batch_buf(batch_data);
            size_t batch_size = 0;
            constexpr size_t max_batch_size = 10'000;
            std::unique_ptr<rocksdb::Iterator> iter{cf_handler->db->NewIterator(read_options, cf_handler->cf_handle)};
            for (iter->SeekToFirst(); iter->Valid(); iter->Next())
            {
                writeString(iter->key().ToStringView(), batch_buf);
                writeString(iter->value().ToStringView(), batch_buf);

                /// Write 10k key / value pairs in one batch
                if (++batch_size == max_batch_size)
                {
                    batch_buf.finalize();
                    writeVarUInt(max_batch_size, wb);
                    writeString(batch_data, wb);
                    batch_buf.restart();
                    batch_size = 0;
                }
            }

            /// Write the last batch
            if (batch_size > 0)
            {
                batch_buf.finalize();
                writeVarUInt(batch_size, wb);
                writeString(batch_data, wb);
            }
        }
        else if (!hot_key_values.empty())
        {
            /// loop each key / value in hot keys and serialize them
            writeVarUInt(hot_key_values.size(), wb);
            for (const auto & [key, entry] : hot_key_values)
            {
                key_serializer(key, wb);
                config.value_serializer(entry.data, wb);
            }
        }

        writeVarUInt(0, wb); /// End with empty batch size

        metrics.serialize(wb, version);
    }

    void read(ReadBuffer & rb, const HybridHashTableConfig::ValueDeserializer & old_value_deserializer = {})
    {
        /// V2 layout : version, cleanup_on_disk_data, batch_size1, key1, value1, key2, value2, batch_size2, ..., empty_batch_size(end), metrics
        VersionType recovered_version;
        readBinary(recovered_version, rb);

        /// Check version compatibility
        static constexpr VersionType min_version = 2;
        if (recovered_version < min_version || recovered_version > version)
            throw Exception(
                ErrorCodes::BAD_VERSION,
                "Failed to recover hybrid hash table, incompatible version: {}, expected version: {}",
                recovered_version,
                version);

        /// FIXME: If cleanup_on_disk_data is false, can we reuse the existing disk data?
        bool cleanup_on_disk_data = false;
        readBinary(cleanup_on_disk_data, rb);
        chassert(cleanup_on_disk_data);

        chassert(hot_key_values.empty() && !persistentPartInited());

        const auto * value_deserializer = old_value_deserializer ? &old_value_deserializer : &config.value_deserializer;
        size_t batch_num = 0;
        do
        {
            readVarUInt(batch_num, rb);
            for (size_t i = 0; i < batch_num; ++i)
            {
                K key;
                if (auto errcode = key_deserializer(key, rb); errcode != ErrorCodes::OK)
                    throw DB::Exception(errcode, "Failed to deserialize key, {}", ErrorCodes::getName(errcode));

                auto value_ptr = constructValue();
                if (auto errcode = (*value_deserializer)(value_ptr.get(), rb); errcode != ErrorCodes::OK)
                    throw DB::Exception(errcode, "Failed to deserialize value, {}", ErrorCodes::getName(errcode));

                insert(key, std::move(value_ptr));

                if (hot_key_values.size() >= config.base_conf.max_hot_key_count)
                {
                    size_t remaining_num_keys = batch_num - i - 1;
                    bulkSpill(std::min(config.base_conf.max_hot_key_count, remaining_num_keys));
                }
            }
        } while (batch_num > 0);

        chassert(recent_keys.size() == hot_key_values.size());

        metrics.deserialize(rb, recovered_version);
    }

    const HybridHashTableMetrics & getMetrics() const noexcept { return metrics; }

    size_t approximateCount() const
    {
        if (full_cached)
            return hot_key_values.size();

        UInt64 estimated_keys = 0;
        if (persistentPartInited())
            cf_handler->db->GetIntProperty(cf_handler->cf_handle, "rocksdb.estimate-num-keys", &estimated_keys);

        return hot_key_values.size() + estimated_keys;
    }

    UInt64 getDiskSize() const
    {
        UInt64 disk_size = 0;
        if (persistentPartInited())
            cf_handler->db->GetIntProperty(cf_handler->cf_handle, "rocksdb.total-sst-files-size", &disk_size);

        return disk_size;
    }

    bool empty() const noexcept { return approximateCount() == 0; }

    size_t getBufferSizeInBytes() const noexcept { return getBufferSizeInCells() * sizeof(typename InmemoryHashMap::value_type); }

    size_t getBufferSizeInCells() const noexcept { return hot_key_values.capacity(); }

    int spillIfNecessary() { return spillIfNecessary(config.base_conf.max_hot_key_count); }

    void flush()
    {
        auto status = bulkSpill(hot_key_values.size(), /*clear_hot_keys=*/false);
        if (status != ErrorCodes::OK)
            throw DB::Exception(status, "Failed to flush hot keys to disk");

        /// After flushing, always expects rocks instance here
        /// In case, no keys in this table
        if (!persistentPartInited())
            initRocks();

        /// Flush all memtables to disk
        rocksdb::FlushOptions flush_opts;
        flush_opts.wait = true;
        cf_handler->db->Flush(flush_opts, cf_handler->cf_handle);
    }

    void reload(const HybridHashTableConfig::ValueDeserializer & old_value_deserializer = {})
    {
        chassert(hot_key_values.empty());
        full_cached = false;

        if (!persistentPartInited())
        {
            initRocks();

            /// If persistent_key_count <= max_hot_key_count, we can consider switching to pure in-memory mode
            /// (i.e. loading all keys directly into memory)
            /// NOTE: If \old_value_deserializer exists, it means the stored value is in the old format,
            /// and we always reload all old key values ​​and then store them in the current value format.
            const auto * value_deserializer = old_value_deserializer ? &old_value_deserializer : &config.value_deserializer;
            if (old_value_deserializer || approximateCount() <= config.base_conf.max_hot_key_count)
            {
                auto scan_options = read_options;
                scan_options.fill_cache = false;
                std::unique_ptr<rocksdb::Iterator> iter{cf_handler->db->NewIterator(scan_options, cf_handler->cf_handle)};
                for (iter->SeekToFirst(); iter->Valid(); iter->Next())
                {
                    K k;
                    {
                        ReadBufferFromMemory rb{iter->key().data(), iter->key().size()};
                        if (auto errcode = key_deserializer(k, rb); errcode != ErrorCodes::OK)
                            return;
                    }

                    auto res = initLoadedValuesAndInsert(k, iter->value().ToStringView(), *value_deserializer);
                    if (res.hasError())
                        return;
                }
                full_cached = true;
                spillIfNecessary();
            }
        }
    }

    /// Low level API
    int spillIfNecessary(size_t current_batch_size)
    {
        auto hot_keys_size = recent_keys.size();
        if (hot_keys_size <= config.base_conf.max_hot_key_count || hot_keys_size <= current_batch_size)
            return ErrorCodes::OK;

        /// After emplaceKey(s), this current batch of keys will be hot (moved to the tail of recent_keys)
        /// we can only spill cold keys which is in front of recent_keys and need make sure
        /// the hot keys in the current batch won't be spilled since they are keys clients like to manipulate.
        /// So the maximum keys to spill is `hot_key_values.size() - current_batch_size`.
        /// It also indicates the total hot keys in memory sometimes can exceed config.max_hot_key_count
        auto max_keys_to_spill = std::min(hot_keys_size - current_batch_size, hot_keys_size - config.base_conf.max_hot_key_count);
        return bulkSpill(max_keys_to_spill);
    }

    void logMetrics(int64_t throttling_sec, std::string_view ht_name, std::string_view ht_id)
    {
        if (auto [should_log, _] = cluster::shouldLog(tracked_logs, /*log_key=*/1, throttling_sec); !should_log)
            return;

        if (persistentPartInited())
        {
            uint64_t rocksdb_size_all_mem_tables = 0;
            uint64_t rocksdb_cur_size_all_mem_tables = 0;
            uint64_t rocksdb_estimate_table_readers_mem = 0;
            uint64_t rocksdb_block_cache_usage = 0;
            uint64_t rocksdb_block_cache_pinned_usage = 0;
            uint64_t rocksdb_memory_usage = 0;

            cf_handler->db->GetIntProperty("rocksdb.size-all-mem-tables", &rocksdb_size_all_mem_tables); // ~kMemTableTotal
            cf_handler->db->GetIntProperty(
                "rocksdb.cur-size-all-mem-tables", &rocksdb_cur_size_all_mem_tables); // ~kMemTableTotal (current snapshot)
            cf_handler->db->GetIntProperty(
                "rocksdb.estimate-table-readers-mem", &rocksdb_estimate_table_readers_mem); // ~kTableReadersTotal
            cf_handler->db->GetIntProperty("rocksdb.block-cache-usage", &rocksdb_block_cache_usage); // ~kCacheTotal
            cf_handler->db->GetIntProperty(
                "rocksdb.block-cache-pinned-usage", &rocksdb_block_cache_pinned_usage); // pinned part of kCacheTotal
            cf_handler->db->GetIntProperty("rocksdb.memory-usage", &rocksdb_memory_usage); // total aggregate

            LOG_INFO(
                logger,
                "HybridHashTable: {}-{} hot_keys={} recent_keys={} approx_keys={} disk_size={} {} rocksdb_size_all_mem_tables={} "
                "rocksdb_cur_size_all_mem_tables={} rocksdb_estimate_table_readers_mem={} rocksdb_block_cache_usage={} "
                "rocksdb_block_cache_pinned_usage={} rocksdb_memory_usage={}",
                ht_name,
                ht_id,
                hot_key_values.size(),
                recent_keys.size(),
                approximateCount(),
                getDiskSize(),
                metrics.string(),
                rocksdb_size_all_mem_tables,
                rocksdb_cur_size_all_mem_tables,
                rocksdb_estimate_table_readers_mem,
                rocksdb_block_cache_usage,
                rocksdb_block_cache_pinned_usage,
                rocksdb_memory_usage);
        }
        else
        {
            LOG_INFO(
                logger,
                "HybridHashTable: {} hot_keys={} recent_keys={} approx_keys={} disk_size={}, {}",
                ht_name,
                hot_key_values.size(),
                recent_keys.size(),
                approximateCount(),
                getDiskSize(),
                metrics.string());
        }
    }

    /// For testing
    auto & hotKeyValues() noexcept { return hot_key_values; }
    const auto & recentKeys() const noexcept { return recent_keys; }
    bool persistentPartInited() const noexcept { return cf_handler != nullptr; }

private:
    void initRocks()
    {
        write_options.disableWAL = config.base_conf.cleanup_on_disk_data;

        /// 1) Shared rocks case: if cf_handler_getter is provided, get rocks handler from external rocksdb instance
        /// For example, `Rocks` has 3 column families : [cf1, cf2, cf3] and one `db`
        ///
        /// HybridHashTable-1 -> cf1, db
        /// HybridHashTable-2 -> cf2, db
        /// HybridHashTable-3 -> cf3, db
        if (config.base_conf.rocks_cf_handler_getter)
        {
            cf_handler = config.base_conf.rocks_cf_handler_getter(config.base_conf);
            return;
        }

        /// 2) Otherwise, create an internal rocksdb instance
        rocksdb::DB * db = nullptr;
        if (auto status = rocksdb::DB::Open(config.getRocksOptions(), config.base_conf.spill_dir_path, &db); !status.ok())
        {
            LOG_ERROR(logger, "Failed to init on disk hash table, status='{}'", status.ToString());
            throw DB::Exception(ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open on disk hash table, {}", status.ToString());
        }

        rocks = std::make_shared<RocksDB>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, config.base_conf.cleanup_on_disk_data, logger);
        cf_handler = rocks->getOrCreateColumnFamilyHandler(config.base_conf.cf_handle_id, config.base_conf.ttl);

        LOG_INFO(
            logger,
            "Init hybrid hash table with ttl={} path={} kv_options={} use_hash_index={}",
            config.base_conf.ttl,
            config.base_conf.spill_dir_path,
            config.base_conf.kv_options,
            config.base_conf.use_hash_index);
    }

    HybridFindResults doFindKeys(std::vector<K>::const_iterator keys_start, std::vector<K>::const_iterator keys_end)
    {
        size_t key_count = std::distance(keys_start, keys_end);

        Stopwatch stopwatch;
        SCOPE_EXIT({
            if (auto elapsed_ms = stopwatch.elapsedMilliseconds(); elapsed_ms > 500)
                LOG_INFO(logger, "Looking up {} keys, took {}ms", key_count, elapsed_ms);
        });

        metrics.read_total += key_count;

        HybridFindResults results;
        results.results.resize(key_count);

        std::vector<size_t> read_key_positions;
        /// read_key_positions.reserve(key_count / 2);

        auto key_iter = keys_start;
        for (size_t i = 0; key_iter != keys_end; ++key_iter, ++i)
        {
            const auto & key = *key_iter;

            if (auto iter = hot_key_values.find(key); iter != hot_key_values.end())
            {
                moveToTail(iter->second);

                results.results[i].value = iter->second;
            }
            else
            {
                /// Record keys which are not in memory which are used to lookup on disk
                read_key_positions.push_back(i);
            }
        }

        if (!persistentPartInited() || read_key_positions.empty())
            return results;

        std::vector<std::vector<char>> read_keys_data;
        read_keys_data.reserve(read_key_positions.size());

        std::vector<rocksdb::Slice> read_key_slices;
        read_key_slices.reserve(read_key_positions.size());

        for (auto pos : read_key_positions)
        {
            read_keys_data.emplace_back();
            auto & key_data = read_keys_data.back();
            {
                WriteBufferFromVector<std::vector<char>> wb(key_data);
                if (auto errcode = key_serializer(*(keys_start + pos), wb); errcode != ErrorCodes::OK)
                    return HybridFindResults{errcode};
            }
            read_key_slices.emplace_back(key_data.data(), key_data.size());
        }

        std::vector<rocksdb::ColumnFamilyHandle *> column_families(read_key_slices.size(), cf_handler->cf_handle);
        std::vector<std::string> values;
        auto statuses = cf_handler->db->MultiGet(read_options, column_families, read_key_slices, &values);

        for (size_t i = 0, statuses_size = statuses.size(); i < statuses_size; ++i)
        {
            const auto & status = statuses[i];
            if (status.ok())
            {
                ++metrics.read_persistent;

                auto source_key_position = read_key_positions[i];
                const auto & key = *(keys_start + source_key_position);

                if (auto iter = hot_key_values.find(key); iter != hot_key_values.end())
                {
                    /// It is possible there are duplicates in the request key range and the first key will
                    /// populate hot_key_values, the remaining same key can just reuse the hot keys
                    results.results[source_key_position].value = iter->second;
                    continue;
                }

                /// We don't like to spill the key / values in the current batch
                /// since if the current batch exceeds the max hot keys, it result in
                /// spill back to disk to make it un-accessible
                auto find_result = initLoadedValuesAndInsert(key, values[i], config.value_deserializer);
                if (!find_result.hasError())
                {
                    chassert(!results.results[source_key_position].isFound());
                    results.results[source_key_position] = std::move(find_result);
                    values[i] = std::string{};
                }
                else
                {
                    return HybridFindResults{find_result.errcode};
                }
            }
            else if (status.IsNotFound())
            {
                /// Do nothing
            }
            else
            {
                LOG_ERROR(logger, "Failed to lookup key on disk, status='{}'", status.ToString());
                return HybridFindResults{ErrorCodes::ROCKSDB_ERROR};
            }
        }

        return results;
    }

    HybridFindResult
    initLoadedValuesAndInsert(const K & key, std::string_view rvalue, const HybridHashTableConfig::ValueDeserializer & value_deserializer)
    {
        /// Loaded from persistent store
        auto value_ptr = constructValue();
        {
            ReadBufferFromString rb{rvalue};
            if (auto errcode = value_deserializer(value_ptr.get(), rb); errcode != ErrorCodes::OK)
                return HybridFindResult{errcode};
        }

        auto & entry = insert(key, std::move(value_ptr), /*from_loaded=*/true);
        return HybridFindResult{entry};
    }

    HybridFindResult doFindKey(const K & key)
    {
        ++metrics.read_total;

        if (auto iter = hot_key_values.find(key); iter != hot_key_values.end())
        {
            moveToTail(iter->second);

            return HybridFindResult{iter->second};
        }

        if (!persistentPartInited())
            return HybridFindResult{};

        /// Try to find in persistent store
        std::vector<char> key_data;
        {
            WriteBufferFromVector<std::vector<char>> wb(key_data);
            if (auto errcode = key_serializer(key, wb); errcode != ErrorCodes::OK)
                return HybridFindResult{errcode};
        }

        std::string rvalue;
        if (auto status
            = cf_handler->db->Get(read_options, cf_handler->cf_handle, rocksdb::Slice{key_data.data(), key_data.size()}, &rvalue);
            status.ok())
        {
            ++metrics.read_persistent;

            return initLoadedValuesAndInsert(key, rvalue, config.value_deserializer);
        }
        else if (status.IsNotFound())
        {
            return HybridFindResult{};
        }
        else
        {
            LOG_ERROR(logger, "Failed to lookup key on disk, status='{}'", status.ToString());
            return HybridFindResult{ErrorCodes::ROCKSDB_ERROR};
        }
    }

    /// \param entry already has ownership of the memory
    ALWAYS_INLINE Entry & insert(const K & key, auto && value_ptr, bool from_loaded = false)
    {
        auto & control = getControlBits(value_ptr.get());
        control.changed = !from_loaded;
        /// control.loaded = from_loaded;

        Entry entry{value_ptr.get(), control};

        auto [iter, inserted] = hot_key_values.emplace(key, entry);
        if (likely(inserted))
        {
            value_ptr.release(); /// hot_key_values owns the memory now

            recent_keys.emplace_back(key);

            /// Update the key position in the map
            iter->second.recent_keys_position = --recent_keys.end();
        }

        chassert(recent_keys.size() == hot_key_values.size());
        return iter->second;
    }

    ALWAYS_INLINE int bulkSpill(size_t number_of_keys_to_spill, bool clear_hot_keys = true)
    {
        chassert(number_of_keys_to_spill <= hot_key_values.size() && number_of_keys_to_spill <= recent_keys.size());

        std::vector<const K *> keys;
        keys.reserve(number_of_keys_to_spill);

        std::vector<Entry *> entries;
        entries.reserve(number_of_keys_to_spill);

        /// Firstly, collect all dirty key / value pairs to flush
        auto iter = recent_keys.begin();
        for (size_t i = 0; i < number_of_keys_to_spill; ++i)
        {
            /// Spill the oldest key / value to disk
            auto miter = hot_key_values.find(*iter);
            chassert(miter != hot_key_values.end());

            if (miter != hot_key_values.end())
            {
                chassert(iter == miter->second.recent_keys_position);

                /// Only spill the dirty entries
                if (miter->second.isDirty())
                {
                    keys.push_back(&miter->first);
                    entries.push_back(&miter->second);
                }
            }

            ++iter;
        }

        /// Secondly, spill to disk if there are dirty key / value pairs
        if (!keys.empty())
        {
            if (auto errcode = spill(std::move(keys), std::move(entries), /*cleanup_after_spill=*/clear_hot_keys);
                errcode != ErrorCodes::OK)
                return errcode;
        }

        /// Thirdly, only spill is successful, then clean up in-memory key / value pairs
        if (clear_hot_keys)
            removeNHotKeyValues(number_of_keys_to_spill);

        chassert(recent_keys.size() == hot_key_values.size());
        return ErrorCodes::OK;
    }

    ALWAYS_INLINE void removeNHotKeyValues(size_t number_of_keys_to_spill)
    {
        full_cached = false;

        /// Fast path
        if (number_of_keys_to_spill == hot_key_values.size())
        {
            for (auto & [_, entry] : hot_key_values)
                destructValue(entry.data);

            hot_key_values.clear();
            recent_keys.clear();
            return;
        }

        chassert(number_of_keys_to_spill <= recent_keys.size());

        for (size_t i = 0; i < number_of_keys_to_spill; ++i)
        {
            /// Spill the oldest key / value to disk
            auto miter = hot_key_values.find(recent_keys.front());
            chassert(miter != hot_key_values.end());
            if (miter != hot_key_values.end())
            {
                destructValue(miter->second.data);
                hot_key_values.erase(miter);
            }

            recent_keys.pop_front();
        }
    }

    int spill(std::vector<const K *> keys, std::vector<Entry *> entries, bool cleanup_after_spilled)
    {
        if (!persistentPartInited())
            initRocks();

        ++metrics.spilled;

        size_t approx_batch_size = 0;

        using CharVector = std::vector<char>;
        using VectorOfCharVector = std::vector<CharVector>;

        VectorOfCharVector keys_data;
        keys_data.reserve(keys.size());
        for (const auto * key : keys)
        {
            keys_data.emplace_back();
            auto & key_data = keys_data.back();
            {
                WriteBufferFromVector<CharVector> wb(key_data);
                if (auto errcode = key_serializer(*key, wb); errcode != ErrorCodes::OK)
                    return errcode;
            }
            approx_batch_size += key_data.size();
        }

        VectorOfCharVector values_data;
        values_data.reserve(keys.size());
        for (auto * entry : entries)
        {
            values_data.emplace_back();
            auto & value_data = values_data.back();
            {
                WriteBufferFromVector<CharVector> wb(value_data);
                if (auto errcode = config.value_serializer(entry->data, wb); errcode != ErrorCodes::OK)
                    return errcode;

                if (cf_handler->ttl_sec > 0)
                {
                    /// Append timestamp to tail
                    auto now_sec = static_cast<UInt32>(DB::UTCSeconds::now());
                    DB::writeIntBinary(now_sec, wb);
                }
            }
            approx_batch_size += value_data.size();
        }

        rocksdb::WriteBatch batch{static_cast<size_t>(approx_batch_size * 1.5)};
        for (size_t i = 0, size = keys.size(); i < size; ++i)
        {
            auto status = batch.Put(
                cf_handler->cf_handle, {keys_data[i].data(), keys_data[i].size()}, {values_data[i].data(), values_data[i].size()});
            if (!status.ok())
            {
                LOG_ERROR(logger, "Failed to add key/value to batch, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }

        /// The data in batch can be moved to other thread when db->Write(...)
        /// which caused the tracking inaccuracy
        /// https://github.com/timeplus-io/proton-enterprise/issues/10675
        [[maybe_unused]] auto _trace = CurrentMemoryTracker::free(batch.GetDataSize());

        if (auto status = cf_handler->db->Write(write_options, &batch); status.ok())
        {
            /// If cleanup_after_spilled is true, we can skip resetting the changed flag for all spilled keys
            if (!cleanup_after_spilled)
            {
                for (auto * entry : entries)
                    entry->control->changed = false;
            }

            return ErrorCodes::OK;
        }
        else
        {
            LOG_ERROR(logger, "Failed to spill key/values to disk, status='{}'", status.ToString());
            return ErrorCodes::ROCKSDB_ERROR;
        }
    }

    /// \return true if key is found and removed, otherwise false
    ALWAYS_INLINE bool removeOneHotKey(const K & key)
    {
        if (auto iter = hot_key_values.find(key); iter != hot_key_values.end())
        {
            recent_keys.erase(iter->second.recent_keys_position);
            destructValue(iter->second.data);
            hot_key_values.erase(iter);
            chassert(recent_keys.size() == hot_key_values.size());
            return true;
        }
        return false;
    }

    ALWAYS_INLINE void destructValue(Mapped value) const
    {
        /// NOTE: If value_destructor throws, there may be memory leak
        config.value_destructor(value);
        /// std::free(value);
        allocator.free(value, config.value_object_size + sizeof(HybridMappedValue::ControlBits));
    }

    ALWAYS_INLINE auto constructValue() const
    {
        /// Allocate memory for value object: value_object_size + control_bits(1 byte)
        /// auto value_ptr = alignedAllocate(config.value_object_size + sizeof(HybridMappedValue::ControlBits), config.align_value_object_size);
        std::unique_ptr<void, decltype(&std::free)> value_ptr{
            allocator.alloc(config.value_object_size + sizeof(HybridMappedValue::ControlBits), config.align_value_object_size), &std::free};
        config.value_constructor(value_ptr.get());
        auto deleter = [this](void * ptr) { destructValue(ptr); };
        return std::unique_ptr<void, decltype(deleter)>(value_ptr.release(), std::move(deleter));
    }

    ALWAYS_INLINE HybridMappedValue::ControlBits & getControlBits(Mapped value) const
    {
        /// The control bits are stored at the end of the value object
        return *reinterpret_cast<HybridMappedValue::ControlBits *>(reinterpret_cast<char *>(value) + config.value_object_size);
    }

    ALWAYS_INLINE void moveToTail(Entry & entry)
    {
        recent_keys.splice(recent_keys.end(), recent_keys, entry.recent_keys_position);
        entry.recent_keys_position = --recent_keys.end();
    }

private:
    HybridHashTableConfig config;
    KeySerializer key_serializer;
    KeyDeserializer key_deserializer;
    mutable Allocator</*clear_memory_=*/false, /*populate=*/false, /*track=*/true> allocator;

    InmemoryHashMap hot_key_values;
    std::list<K> recent_keys;
    bool full_cached = true;

    HybridHashTableMetrics metrics;

    RocksDBPtr rocks; /// internal rocksdb instance
    RocksDBColumnFamilyHandlerPtr cf_handler;

    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;

    cluster::LogTrackContainer tracked_logs;

    LoggerPtr logger;
};

}
