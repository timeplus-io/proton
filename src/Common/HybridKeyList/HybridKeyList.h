#pragma once

#include <IO/PrefixTreeEncode.h>
#include <base/ClockUtils.h>
#include <Common/ErrorCodes.h>
#include <Common/HybridConfig.h>
#include <Common/Rocks/RocksDBTTLCompactionFilter.h>
#include <Common/logger_useful.h>

#include <absl/container/flat_hash_set.h>
#include <rocksdb/db.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>

#include <filesystem>
#include <set>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_OPEN_DATABASE;
extern const int OK;
}

/// HybridKeyList is an ascending sorted list by timestamp and the key
/// It doesn't check the existence / duplication of the keys when inserting
template <typename K>
struct HybridKeyList
{
public:
    using KeyType = K;

    using KeySerializer = std::function<int(const K &, WriteBuffer &)>;
    using KeyDeserializer = std::function<int(K &, ReadBuffer &)>;

    struct KeyWithTimestamp
    {
        KeyWithTimestamp(K && k_, int64_t ts_) : k(std::move(k_)), ts(ts_) { }
        KeyWithTimestamp(const K & k_, int64_t ts_) : k(k_), ts(ts_) { }

        bool expired(int64_t timeout_threshold_ms) const noexcept { return UTCMilliseconds::now() > ts + timeout_threshold_ms; }

        K k;
        int64_t ts;
    };

    struct KeyWithTimestampLess
    {
        bool operator()(const KeyWithTimestamp & lhs, const KeyWithTimestamp & rhs) const noexcept
        {
            if (lhs.ts < rhs.ts)
                return true;

            if (lhs.ts > rhs.ts)
                return false;

            return lhs.k < rhs.k;
        }
    };

    HybridKeyList(HybridConfig config_, KeySerializer key_serializer_, KeyDeserializer key_deserializer_, LoggerPtr logger_)
        : config(std::move(config_))
        , key_serializer(std::move(key_serializer_))
        , key_deserializer(std::move(key_deserializer_))
        , logger(logger_)
    {
        config.ttl = 0; /// Disable ttl for hybrid key list

        chassert(key_serializer && key_deserializer);
        if (std::filesystem::exists(config.spill_dir_path)
            && !config.rocks_cf_handler_getter) /// If unshared rocks directory already exists, init rocks eagerly, otherwise in a lazy way)
            reload();
    }

    ~HybridKeyList()
    {
        try
        {
            clear();
        }
        catch (...)
        {
            tryLogCurrentException(logger, "Exception happened in ~HybridKeyList");
        }
    }

    /// Emplace back a key without checking if the key existence, if max cold keys reaches the threshold,
    /// the new key will be inserted to hybrid hash table which will be spilled to disk
    /// \return {create_utc_ts, errcode} pair
    std::pair<int64_t, int> emplaceKey(const K & key)
    {
        auto ts = DB::UTCMilliseconds::now();
        if (oldest_keys.size() < config.max_hot_key_count)
        {
            oldest_keys.emplace(key, ts);
            return {ts, ErrorCodes::OK};
        }
        else
        {
            /// Spill to disk since the in-memory part is full
            if (!persistentPartInited())
                initRocks();

            auto encode_result = encodeKey(key, ts);
            if (encode_result.second != ErrorCodes::OK)
                return {0, encode_result.second};

            if (auto status = cf_handler->db->Put(write_options, cf_handler->cf_handle, encode_result.first, rocksdb::Slice{}); status.ok())
            {
                return {ts, ErrorCodes::OK};
            }
            else
            {
                LOG_ERROR(logger, "Failed to spill key to disk, status='{}'", status.ToString());
                return {0, ErrorCodes::ROCKSDB_ERROR};
            }
        }
    }

    /// \return {create_utc_ts, errcode} pair
    std::pair<int64_t, int> emplaceKeys(std::vector<K> & keys)
    {
        auto ts = DB::UTCMilliseconds::now();

        size_t new_size = keys.size() + oldest_keys.size();
        if (new_size < config.max_hot_key_count)
        {
            for (auto & key : keys)
                oldest_keys.emplace(std::move(key), ts);

            return {ts, ErrorCodes::OK};
        }
        else
        {
            size_t i = 0;
            for (; oldest_keys.size() < config.max_hot_key_count; ++i)
                oldest_keys.emplace(std::move(keys[i]), ts);

            /// Spill to disk since the in-memory part is full

            if (!persistentPartInited())
                initRocks();

            /// Flush remaining keys to disk
            rocksdb::WriteBatch batch{keys.size() * (64 + 8)};
            for (size_t size = keys.size(); i < size; ++i)
            {
                auto encode_result = encodeKey(keys[i], ts);
                if (encode_result.second != ErrorCodes::OK)
                    return {0, encode_result.second};

                auto status = batch.Put(cf_handler->cf_handle, encode_result.first, rocksdb::Slice{});
                if (!status.ok())
                {
                    LOG_ERROR(logger, "Failed to add key/value to batch, status='{}'", status.ToString());
                    return {0, ErrorCodes::ROCKSDB_ERROR};
                }
            }

            /// The data in batch can be moved to other thread when db->Write(...)
            /// which caused the tracking inaccuracy
            /// https://github.com/timeplus-io/proton-enterprise/issues/10675
            [[maybe_unused]] auto _trace = CurrentMemoryTracker::free(batch.GetDataSize());

            if (auto status = cf_handler->db->Write(write_options, &batch); !status.ok())
            {
                LOG_ERROR(logger, "Failed to spill key/values to disk, status='{}'", status.ToString());
                return {0, ErrorCodes::ROCKSDB_ERROR};
            }

            return {ts, ErrorCodes::OK};
        }
    }

    /// \return ErrorCodes::OK if successful, otherwise errcode
    int removeKey(const K & key, int64_t create_utc_ts)
    {
        oldest_keys.remove(key, create_utc_ts);

        if (persistentPartInited())
        {
            /// Remove from persistent store
            auto encode_result = encodeKey(key, create_utc_ts);
            if (encode_result.second != ErrorCodes::OK)
                return encode_result.second;

            if (auto status = cf_handler->db->Delete(write_options, encode_result.first); !status.ok())
            {
                LOG_ERROR(
                    logger, "Failed to delete key from persistent store, key_create_ts={} status='{}'", create_utc_ts, status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }

        return ErrorCodes::OK;
    }

    /// Find timed-out keys, remove them from the key list
    /// Either all timed-out keys are removed or \max_keys_to_remove threshold reaches
    /// \return {timed-out_keys, errcode}
    std::pair<std::vector<K>, int>
    removeTimedOutKeys(int64_t timeout_threshold_ms, const absl::flat_hash_set<K> & skipped_keys, size_t max_keys_to_remove = 100'000)
    {
        if (empty())
            return {{}, ErrorCodes::OK};

        std::pair<std::vector<K>, int> result;
        result.second = ErrorCodes::OK;

        bool has_on_disk_keys = persistentPartInited();

        /// return DB::ErrorCodes::OK if no error, otherwise errcode
        auto remove_expires_from_memory = [&](absl::flat_hash_set<std::string> & expired_encoded_keys, size_t approx_batch_size) {
            for (auto iter = oldest_keys.keys.begin(); iter != oldest_keys.keys.end() && result.first.size() < max_keys_to_remove;)
            {
                if (skipped_keys.contains(iter->k))
                {
                    /// Already handled
                    if (has_on_disk_keys)
                    {
                        auto encode_result = encodeKey(iter->k, iter->ts);
                        if (encode_result.second != ErrorCodes::OK)
                            return encode_result.second;

                        approx_batch_size += encode_result.first.size();
                        expired_encoded_keys.insert(std::move(encode_result.first));
                    }

                    iter = oldest_keys.keys.erase(iter);
                }
                else if (iter->expired(timeout_threshold_ms))
                {
                    if (has_on_disk_keys)
                    {
                        auto encode_result = encodeKey(iter->k, iter->ts);
                        if (encode_result.second != ErrorCodes::OK)
                            return encode_result.second;

                        approx_batch_size += encode_result.first.size();
                        expired_encoded_keys.insert(std::move(encode_result.first));
                    }

                    result.first.push_back(std::move(iter->k));

                    iter = oldest_keys.keys.erase(iter);
                }
                else
                {
                    /// Key list is monotonically sorted
                    return ErrorCodes::OK;
                }
            }

            return ErrorCodes::OK;
        };

        auto remove_from_disk = [&](const absl::flat_hash_set<std::string> & encoded_removed_keys, size_t reserved_batch_size) {
            rocksdb::WriteBatch batch{static_cast<size_t>(reserved_batch_size * 1.2)};

            for (const auto & encoded_key : encoded_removed_keys)
                batch.Delete(cf_handler->cf_handle, encoded_key);

            if (auto status = cf_handler->db->Write(write_options, &batch); status.ok())
            {
                return ErrorCodes::OK;
            }
            else
            {
                LOG_ERROR(logger, "Failed to delete keys from disk, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        };

        bool on_disk_keys_drained = false;
        do
        {
            absl::flat_hash_set<std::string> expired_encoded_keys;
            size_t approx_batch_size = 0;
            if (auto errcode = remove_expires_from_memory(expired_encoded_keys, approx_batch_size); errcode != ErrorCodes::OK)
            {
                result.second = errcode;
                return result;
            }

            if (!expired_encoded_keys.empty())
            {
                if (auto errcode = remove_from_disk(expired_encoded_keys, approx_batch_size); errcode != ErrorCodes::OK)
                {
                    result.second = errcode;
                    return result;
                }

                bool may_have_more_expired_keys = oldest_keys.empty() && !on_disk_keys_drained;

                if (has_on_disk_keys && !on_disk_keys_drained)
                    /// Try refill more keys from disk to make the in-memory sort list full
                    on_disk_keys_drained = refill();

                if (!may_have_more_expired_keys)
                    return result;
            }
            else
            {
                /// No more expiration keys
                if (has_on_disk_keys && !on_disk_keys_drained)
                    /// Try refill more keys from disk to make the in-memory sort list full
                    on_disk_keys_drained = refill();

                return result;
            }
        } while (result.first.size() < max_keys_to_remove);

        if (result.first.size() >= max_keys_to_remove && !on_disk_keys_drained)
            /// Try last refill to make make the in-memory sort list full
            refill();

        return result;
    }

    bool empty() const { return oldest_keys.empty(); }

    int flush()
    {
        if (oldest_keys.empty())
            return ErrorCodes::OK;

        if (!persistentPartInited())
            initRocks();

        /// Flush everything to disk
        rocksdb::WriteBatch batch;
        for (const auto & key_ts : oldest_keys.keys)
        {
            auto encode_result = encodeKey(key_ts.k, key_ts.ts);
            if (encode_result.second != ErrorCodes::OK)
                return encode_result.second;

            auto status = batch.Put(cf_handler->cf_handle, encode_result.first, rocksdb::Slice{});
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

        return ErrorCodes::OK;
    }

    void clear()
    {
        if (!config.cleanup_on_disk_data)
        {
            /// If we like to retain the data around, flush them to disk
            flush();
        }
        else
        {
            destroyPersistentPart();
        }

        oldest_keys.clear();

        cf_handler.reset();
        rocks.reset();
    }

    void destroyPersistentPart()
    {
        if (!persistentPartInited())
            return;

        /// For shared rocks, we just destroy current handler and column family
        if (!rocks && cf_handler && config.cleanup_on_disk_data)
            cf_handler->destroy();

        cf_handler.reset();
        rocks.reset();
    }

    size_t approximateCount() const
    {
        UInt64 estimated_keys = 0;
        if (persistentPartInited())
            cf_handler->db->GetIntProperty(cf_handler->cf_handle, "rocksdb.estimate-num-keys", &estimated_keys);

        return oldest_keys.size() + estimated_keys;
    }

    UInt64 getDiskSize() const
    {
        UInt64 disk_size = 0;
        if (persistentPartInited())
            cf_handler->db->GetIntProperty(cf_handler->cf_handle, "rocksdb.total-sst-files-size", &disk_size);

        return disk_size;
    }

    size_t getBufferSizeInBytes() const noexcept { return getBufferSizeInCells() * sizeof(typename std::list<K>::value_type); }

    size_t getBufferSizeInCells() const noexcept { return oldest_keys.size(); }

    void read(ReadBuffer & rb) { (void)rb; }

    void write(WriteBuffer & wb) const { (void)wb; }

    /// For testing
    /// User can do flush first and then loop keys on disk
    int forEachPersistent(std::function<void(const K &, int64_t ts)> callback) const
    {
        if (!persistentPartInited())
            return DB::ErrorCodes::OK;

        std::unique_ptr<rocksdb::Iterator> iterator(cf_handler->db->NewIterator(read_options, cf_handler->cf_handle));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
        {
            int64_t ts = 0;
            K key;
            if (auto errcode = decodeKey(iterator->key().ToStringView(), key, ts); errcode != ErrorCodes::OK)
                return errcode;

            callback(key, ts);
        }

        return ErrorCodes::OK;
    }

    int forEach(std::function<void(const K &, int64_t ts)> callback) const
    {
        if (!persistentPartInited())
        {
            for (const auto & key_ts : oldest_keys.keys)
                callback(key_ts.k, key_ts.ts);
        }
        else
        {
            absl::flat_hash_set<std::string> handled;

            for (const auto & key_ts : oldest_keys.keys)
            {
                callback(key_ts.k, key_ts.ts);
                auto encode_result = encodeKey(key_ts.k, key_ts.ts);
                if (encode_result.second != ErrorCodes::OK)
                    return encode_result.second;

                handled.insert(std::move(encode_result.first));
            }

            std::unique_ptr<rocksdb::Iterator> iterator(cf_handler->db->NewIterator(read_options, cf_handler->cf_handle));
            for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
            {
                auto key_v = iterator->key().ToStringView();
                if (handled.contains(key_v))
                    continue;

                int64_t ts = 0;
                K key;
                if (auto errcode = decodeKey(key_v, key, ts); errcode != ErrorCodes::OK)
                    return errcode;

                callback(key, ts);
            }
        }

        return ErrorCodes::OK;
    }

    bool persistentPartInited() const noexcept { return cf_handler != nullptr; }

    /// reloaded all keys from disk
    void reload() { refill(); }

private:
    void initRocks()
    {
        write_options.disableWAL = config.cleanup_on_disk_data;

        /// 1) Shared rocks case: if rocks_cf_handler_getter is provided, get rocks cf handler from external rocksdb instance
        /// For example, `Rocks` has 3 column families : [cf1, cf2, cf3] and one `db`
        ///
        /// HybridHashTable-1 -> cf1, db
        /// HybridHashTable-2 -> cf2, db
        /// HybridHashTable-3 -> cf3, db
        if (config.rocks_cf_handler_getter)
        {
            cf_handler = config.rocks_cf_handler_getter(config);
            return;
        }

        rocksdb::DB * db = nullptr;
        if (auto status = rocksdb::DB::Open(config.getRocksOptions(), config.spill_dir_path, &db); !status.ok())
        {
            LOG_ERROR(logger, "Failed to init on disk sorted list, status='{}'", status.ToString());
            throw DB::Exception(ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open on disk sorted list, {}", status.ToString());
        }

        rocks = std::make_shared<RocksDB>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, config.cleanup_on_disk_data, logger);
        cf_handler = rocks->getOrCreateColumnFamilyHandler(config.cf_handle_id, config.ttl);
    }

    int refill()
    {
        if (!persistentPartInited())
            initRocks();

        std::unique_ptr<rocksdb::Iterator> iterator(cf_handler->db->NewIterator(read_options, cf_handler->cf_handle));
        for (iterator->SeekToFirst(); iterator->Valid() && oldest_keys.size() < config.max_hot_key_count; iterator->Next())
        {
            auto key_v = iterator->key().ToStringView();

            int64_t ts = 0;
            K key;
            if (auto errcode = decodeKey(key_v, key, ts); errcode != ErrorCodes::OK)
                throw DB::Exception(errcode, "Failed to deserialize key, {}", ErrorCodes::getName(errcode));

            oldest_keys.emplace(std::move(key), ts);
        }

        return !iterator->Valid();
    }

    int decodeKey(std::string_view key_v, K & key, int64_t & ts) const
    {
        ts = PrefixTreeEncode::decodeVarIntAscending(key_v);
        DB::ReadBufferFromString rb{key_v};

        return key_deserializer(key, rb);
    }

    /// Prefix encode ts and concat serialized key
    /// We use prefixing encoding for ts to make sure after encoding the order still holds
    /// a.k.a smaller ts is ordered before bigger ts after encoding
    /// \return {encoded_key, errcode}
    std::pair<std::string, int> encodeKey(const K & key, uint64_t ts) const
    {
        std::pair<std::string, int> result;
        auto & encoded_key = result.first;
        encoded_key.reserve(256);

        PrefixTreeEncode::encodeVarIntAscending(ts, encoded_key);
        {
            std::string key_data;
            key_data.reserve(256);
            WriteBufferFromString wb{key_data};
            if (auto errcode = key_serializer(key, wb); errcode != ErrorCodes::OK)
            {
                result.second = errcode;
                return result;
            }

            /// Concat ts_data and key_data
            encoded_key += key_data;
        }

        return result;
    }

private:
    struct SortedKeyList
    {
        bool empty() const noexcept { return keys.empty(); }

        size_t size() const noexcept { return keys.size(); }

        void emplace(K && k, int64_t timestamp) { keys.emplace(std::move(k), timestamp); }
        void emplace(const K & k, int64_t timestamp) { keys.emplace(k, timestamp); }

        void clear() { keys.clear(); }

        const KeyWithTimestamp & front() const
        {
            chassert(!keys.empty());
            return *keys.begin();
        }

        /// \return true if removed, otherwise false
        bool remove(const K & k, int64_t timestamp) { return keys.erase(KeyWithTimestamp{k, timestamp}) == 1; }

        std::set<KeyWithTimestamp, KeyWithTimestampLess> keys;
    };

    HybridConfig config;
    KeySerializer key_serializer;
    KeyDeserializer key_deserializer;

    SortedKeyList oldest_keys;

    RocksDBPtr rocks; /// internal rocksdb instance
    RocksDBColumnFamilyHandlerPtr cf_handler;

    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;

    LoggerPtr logger;
};

}
