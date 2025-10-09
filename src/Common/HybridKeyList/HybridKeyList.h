#pragma once

#include <IO/PrefixTreeEncode.h>
#include <base/ClockUtils.h>
#include <Common/Rocks/RocksHandler.h>
#include <Common/logger_useful.h>

#include <absl/container/flat_hash_set.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>

#include <filesystem>
#include <list>

namespace rocksdb
{
class DB;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_OPEN_DATABASE;
extern const int INVALID_CONFIG_PARAMETER;
extern const int OK;
}

/// HybridKeyList is an ascending sorted list by timestamp and the key
/// It doesn't check the existence / duplication of the keys when inserting
struct HybridKeyListConfig
{
    void validate()
    {
        if (spill_dir_path.empty())
            throw DB::Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "HybridHashTable: spill to disk folder is not configured");

        if (max_hot_key_count == 0)
            max_hot_key_count = std::numeric_limits<size_t>::max();
    }

    rocksdb::Options getRocksOptions() const
    {
        rocksdb::Options options;
        options.atomic_flush = true;
        /// options.num_levels = 3;
        options.create_if_missing = true;
        options.create_missing_column_families = true;
        options.statistics = rocksdb::CreateDBStatistics();
        options.info_log_level = rocksdb::ERROR_LEVEL;

        options.compression = rocksdb::CompressionType::kLZ4Compression;

        rocksdb::Options merged_options;
        if (auto status = rocksdb::GetDBOptionsFromString(rocksdb::ConfigOptions{}, options, db_options, &merged_options); !status.ok())
            merged_options = options;

        rocksdb::BlockBasedTableOptions table_options;

        if (use_hash_index)
        {
            table_options.data_block_hash_table_util_ratio = 0.75;
            table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
        }
        else
        {
            table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch;
        }

        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        merged_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        return merged_options;
    }

    /// spill_dir_path_ file system path which holds for spill-to-disk key / values
    std::string spill_dir_path;
    /// db_options_ spill-to-disk (rocks) db options
    std::string db_options;

    /// When ttl <= 0, it means infinity
    int32_t ttl = -1;

    /// If \use_hash_index is true, Binary & hash index will be used, otherwise only binary search will be used.
    /// Hash index usually have better point query perf but will occupy more space
    bool use_hash_index = false;

    /// When in-memory keys count exceed this threshold, spill to disk
    size_t max_hot_key_count = 10'000;
    /// cleanup_on_disk_data_ if true, during dtor, cleanup spill-to-disk data, otherwise keep it around
    bool cleanup_on_disk_data = true;

    /// If `rocks_handler_getter` is set, we will get rocks handler by it
    std::string handle_id{};
    std::function<RocksHandlerPtr(const std::string & id)> rocks_handler_getter{};
};

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

        bool expired(int64_t idle_threshold_ms) const noexcept { return UTCMilliseconds::now() > ts + idle_threshold_ms; }

        K k;
        int64_t ts;
    };

    HybridKeyList(HybridKeyListConfig config_, KeySerializer key_serializer_, KeyDeserializer key_deserializer_, LoggerPtr logger_)
        : config(std::move(config_))
        , key_serializer(std::move(key_serializer_))
        , key_deserializer(std::move(key_deserializer_))
        , logger(logger_)
    {
        chassert(key_serializer && key_deserializer);
        if (std::filesystem::exists(config.spill_dir_path)
            && !config.rocks_handler_getter) /// If unshared rocks directory already exists, init rocks eagerly, otherwise in a lazy way)
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
    int emplace(const K & key)
    {
        auto ts = DB::UTCMilliseconds::now();
        if (oldest_keys.size() < config.max_hot_key_count)
        {
            oldest_keys.emplace(key, ts);
            return ErrorCodes::OK;
        }
        else
        {
            /// Spill to disk since the in-memory part is full
            if (!persistentPartInited())
                initRocks();

            auto encode_result = encodeKey(key, ts);
            if (encode_result.second != ErrorCodes::OK)
                return encode_result.second;

            if (auto status = rocks_handler->db->Put(write_options, rocks_handler->cf_handle, encode_result.first, rocksdb::Slice{}); status.ok())
            {
                return ErrorCodes::OK;
            }
            else
            {
                LOG_ERROR(logger, "Failed to spill key to disk, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }
    }

    int emplace(std::vector<K> & keys)
    {
        auto ts = DB::UTCMilliseconds::now();

        size_t new_size = keys.size() + oldest_keys.size();
        if (new_size < config.max_hot_key_count)
        {
            for (auto & key : keys)
                oldest_keys.emplace(std::move(key), ts);

            return ErrorCodes::OK;
        }
        else
        {
            size_t i = 0;
            for (; oldest_keys.size() < config.max_hot_key_count; ++i)
                oldest_keys.emplace(std::move(keys[i]), ts);

            /// Spill to disk since the in-memory part is full

            if (!persistentPartInited())
                initRocks();

            /// Flush everything to disk
            rocksdb::WriteBatch batch;
            for (size_t size = keys.size(); i < size; ++i)
            {
                auto encode_result = encodeKey(keys[i], ts);
                if (encode_result.second != ErrorCodes::OK)
                    return encode_result.second;

                auto status = batch.Put(rocks_handler->cf_handle, encode_result.first, rocksdb::Slice{});
                if (!status.ok())
                {
                    LOG_ERROR(logger, "Failed to add key/value to batch, status='{}'", status.ToString());
                    return ErrorCodes::ROCKSDB_ERROR;
                }
            }

            if (auto status = rocks_handler->db->Write(write_options, &batch); !status.ok())
            {
                LOG_ERROR(logger, "Failed to spill key/values to disk, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }

            return ErrorCodes::OK;
        }
    }

    /// Find all expired keys, remove them from the key list
    /// \return {expired_keys, errcode}
    std::pair<std::vector<K>, int> removeExpiredKeys(int64_t idle_threshold_ms, const absl::flat_hash_set<K> & handled_expires)
    {
        if (empty())
            return {{}, ErrorCodes::OK};

        std::pair<std::vector<K>, int> result;
        result.second = ErrorCodes::OK;

        absl::flat_hash_set<std::string> removed;
        bool log_removed = persistentPartInited();
        size_t approx_batch_size = 0;

        /// return true if done (means no further processing), otherwise false
        auto remove_expires = [&]() {
            for (auto iter = oldest_keys.keys.begin(); iter != oldest_keys.keys.end();)
            {
                if (handled_expires.contains(iter->k))
                {
                    /// Already handled
                    if (log_removed)
                    {
                        auto encode_result = encodeKey(iter->k, iter->ts);
                        if (encode_result.second != ErrorCodes::OK)
                        {
                            result.second = encode_result.second;
                            return true;
                        }

                        approx_batch_size += encode_result.first.size();
                        removed.insert(std::move(encode_result.first));
                    }

                    iter = oldest_keys.keys.erase(iter);
                }
                else if (iter->expired(idle_threshold_ms))
                {
                    if (log_removed)
                    {
                        auto encode_result = encodeKey(iter->k, iter->ts);
                        if (encode_result.second != ErrorCodes::OK)
                        {
                            result.second = encode_result.second;
                            return true;
                        }

                        approx_batch_size += encode_result.first.size();
                        removed.insert(std::move(encode_result.first));
                    }

                    result.first.push_back(std::move(iter->k));

                    iter = oldest_keys.keys.erase(iter);
                }
                else
                {
                    /// Key list is monotonically sorted
                    return true;
                }
            }

            return false;
        };

        if (auto done = remove_expires(); done)
            return result;

        chassert(oldest_keys.empty());
        if (!persistentPartInited())
            return result;

        /// Check if persistent keys have been expired as well
        for (;;)
        {
            reload(removed);
            if (oldest_keys.empty())
                /// No more keys on disk
                break;

            if (auto done = remove_expires(); done)
                break;
        }

        if (result.second != ErrorCodes::OK)
            return result;

        /// Remove from disk
        rocksdb::WriteBatch batch{static_cast<size_t>(approx_batch_size * 1.2)};

        for (const auto & encode_key : removed)
            batch.Delete(rocks_handler->cf_handle, encode_key);

        if (auto status = rocks_handler->db->Write(write_options, &batch); !status.ok())
        {
            LOG_ERROR(logger, "Failed to delete keys from disk, status='{}'", status.ToString());
            result.second = ErrorCodes::ROCKSDB_ERROR;
        }

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

            auto status = batch.Put(rocks_handler->cf_handle, encode_result.first, rocksdb::Slice{});
            if (!status.ok())
            {
                LOG_ERROR(logger, "Failed to add key/value to batch, status='{}'", status.ToString());
                return ErrorCodes::ROCKSDB_ERROR;
            }
        }

        if (auto status = rocks_handler->db->Write(write_options, &batch); !status.ok())
        {
            LOG_ERROR(logger, "Failed to spill key/values to disk, status='{}'", status.ToString());
            return ErrorCodes::ROCKSDB_ERROR;
        }

        return ErrorCodes::OK;
    }

    void clear()
    {
        if (!config.cleanup_on_disk_data)
            /// If we like to retain the data around, flush them to disk
            flush();

        oldest_keys.clear();

        rocks_handler.reset();
        rocks.reset();
    }

    void destroyPersistentPart()
    {
        if (!persistentPartInited())
            return;

        /// For shared rocks, we just destroy current handler and column family
        if (!rocks && rocks_handler && config.cleanup_on_disk_data)
            rocks_handler->destroy();

        rocks_handler.reset();
        rocks.reset();
    }

    size_t approximateCount() const
    {
        UInt64 estimated_keys = 0;
        if (persistentPartInited())
            rocks_handler->db->GetIntProperty(rocks_handler->cf_handle, "rocksdb.estimate-num-keys", &estimated_keys);

        return oldest_keys.size() + estimated_keys;
    }

    UInt64 getDiskSize() const
    {
        UInt64 disk_size = 0;
        if (persistentPartInited())
            rocks_handler->db->GetIntProperty(rocks_handler->cf_handle, "rocksdb.total-sst-files-size", &disk_size);

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

        std::unique_ptr<rocksdb::Iterator> iterator(rocks_handler->db->NewIterator(read_options, rocks_handler->cf_handle));
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

            std::unique_ptr<rocksdb::Iterator> iterator(rocks_handler->db->NewIterator(read_options, rocks_handler->cf_handle));
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

    void reload() { reload(absl::flat_hash_set<std::string>{}); }

    bool persistentPartInited() const noexcept { return rocks_handler != nullptr; }

private:
    void initRocks()
    {
        write_options.disableWAL = config.cleanup_on_disk_data;

        /// 1) Shared rocks case: if rocks_handler_getter is provided, get rocks handler from external rocksdb instance
        /// For example, `Rocks` has 3 column families : [cf1, cf2, cf3] and one `db`
        ///
        /// HybridHashTable-1 -> cf1, db
        /// HybridHashTable-2 -> cf2, db
        /// HybridHashTable-3 -> cf3, db
        if (config.rocks_handler_getter)
        {
            rocks_handler = config.rocks_handler_getter(config.handle_id);
            return;
        }

        auto options = config.getRocksOptions();
        rocksdb::Status status;
        rocksdb::DB * db = nullptr;

        if (config.ttl > 0)
        {
            rocksdb::DBWithTTL * ttl_db = nullptr;
            status = rocksdb::DBWithTTL::Open(options, config.spill_dir_path, &ttl_db, config.ttl);
            db = ttl_db;
        }
        else
        {
            status = rocksdb::DB::Open(options, config.spill_dir_path, &db);
        }

        if (!status.ok())
        {
            LOG_ERROR(logger, "Failed to init on disk sorted list, status='{}'", status.ToString());
            throw DB::Exception(ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open on disk sorted list, {}", status.ToString());
        }

        rocks = std::make_shared<Rocks>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, config.cleanup_on_disk_data, logger);
        rocks_handler = rocks->getOrCreateHandler(config.handle_id);
    }

    void reload(const absl::flat_hash_set<std::string> & skips)
    {
        chassert(oldest_keys.empty());

        if (!persistentPartInited())
            initRocks();

        std::unique_ptr<rocksdb::Iterator> iterator(rocks_handler->db->NewIterator(read_options, rocks_handler->cf_handle));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next())
        {
            auto key_v = iterator->key().ToStringView();
            if (skips.contains(key_v))
                continue;

            int64_t ts = 0;
            K key;
            if (auto errcode = decodeKey(key_v, key, ts); errcode != ErrorCodes::OK)
                throw DB::Exception(errcode, "Failed to deserialize key, {}", ErrorCodes::getName(errcode));

            if (oldest_keys.size() < config.max_hot_key_count)
                oldest_keys.emplace(std::move(key), ts);
        }
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
    struct KeyList
    {
        bool empty() const noexcept { return keys.empty(); }

        size_t size() const noexcept { return keys.size(); }

        void emplace(K && k, int64_t timestamp) { keys.emplace_back(std::move(k), timestamp); }
        void emplace(const K & k, int64_t timestamp) { keys.emplace_back(k, timestamp); }

        void clear() { keys.clear(); }

        const KeyWithTimestamp & front() const
        {
            chassert(!keys.empty());
            return keys.front();
        }

        void removeFront()
        {
            chassert(!keys.empty());
            keys.pop_front();
        }

        /// Here we are using std::list instead of a std::map or the like
        /// because our current use case is highly temporal: later insert
        /// key is always fresher and we don't have `lookup` requirement
        /// for HybridKeyList but only sequentially accessing the elements
        /// from earliest to latest order to decide if a key is expired.
        /// If we have fast `lookup` requirement or if we like a more general
        /// sorted key space, we may need change std::list to std::map
        std::list<KeyWithTimestamp> keys;
    };

    HybridKeyListConfig config;
    KeySerializer key_serializer;
    KeyDeserializer key_deserializer;

    KeyList oldest_keys;

    RocksPtr rocks; /// internal rocksdb instance
    RocksHandlerPtr rocks_handler;

    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;

    LoggerPtr logger;
};
}
