#pragma once

#include <V8/Modules/DictionaryAccess/DictionaryKeyManager.h>
#include <V8/Modules/DictionaryAccess/DictionaryValueConverter.h>

#include <Core/Block.h>
#include <Dictionaries/DirectDictionary.h>
#include <Common/SharedMutex.h>

#include <absl/container/flat_hash_map.h>

namespace Poco
{
class Logger;
}

namespace DB::V8
{

/// Bridge between V8 and Direct Dictionaries that supports complex keys
class CacheDictionaryBridge
{
public:
    /// Mutually consistent copy of all cached state for one dictionary, taken under a
    /// single lock. Never hold references into the bridge's maps across the lock: the
    /// maps are flat_hash_maps shared by every query thread, and a concurrent load or
    /// invalidation of any dictionary can rehash/erase and invalidate them in place.
    struct DictionarySnapshot
    {
        std::shared_ptr<DB::IDictionary> dictionary;
        std::shared_ptr<DictionaryKeyManager> key_manager;
        std::shared_ptr<DictionaryValueConverter> value_converter;
        Block sample_block;
        DB::StorageID source_db_and_table = DB::StorageID::createEmpty();
        bool is_simple_key = false;
    };

    static CacheDictionaryBridge & instance();

    /// Initialize the bridge with a context
    void initialize(ContextMutablePtr context);

    /// V8 callback implementations
    static void getCardinality(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void getValue(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void setValue(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void batchGetValues(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void batchSetValues(const v8::FunctionCallbackInfo<v8::Value> & args);

    /// Dictionary loading and access
    bool loadDictionary(const std::string & dict_name);
    std::shared_ptr<DB::IDictionary> getDictionary(const std::string & dict_name) const;
    DictionarySnapshot getDictionarySnapshot(const std::string & dict_name) const;
    bool hasDictionary(const std::string & dict_name) const;

    /// Ensure cached dictionary and its metadata are fresh against current table (detect table recreate/UUID change)
    /// Returns true if dictionary is available and fresh after this call (may reload), false if load failed
    bool ensureFreshDictionary(const std::string & dict_name);

    /// Column value conversion
    v8::Local<v8::Value> columnValueToV8(v8::Isolate * isolate, const IColumn & column, size_t row);
    using ColumnConverter = std::function<v8::Local<v8::Value>(v8::Isolate *, const IColumn &, size_t)>;
    const absl::flat_hash_map<TypeIndex, ColumnConverter> & getColumnConverters();

private:
    CacheDictionaryBridge() = default;

    /// Convert a JS number/string simple key to UInt64. Rejects NaN, Infinity, negative
    /// and out-of-uint64-range numbers (whose static_cast would be UB) and unparsable
    /// strings. Returns false with `error` filled; does not throw a JS exception itself.
    static bool jsSimpleKeyToUInt64(v8::Isolate * isolate, v8::Local<v8::Value> key, UInt64 & out, std::string & error);

    /// Read a JS array of projection column names. Returns false if an element access
    /// throws in JS (`error` stays empty, a JS exception is pending) or an element is
    /// not convertible to a string (`error` filled).
    static bool readProjectionColumns(
        v8::Isolate * isolate, v8::Local<v8::Context> context, v8::Local<v8::Array> columns_array, Names & out, std::string & error);

    /// Dictionary and block initialization
    bool initializeSampleBlock(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr);
    bool initializeDictionaryHelpers(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr);

    void InsertToBackendStorage(Block && block, const DB::StorageID & table_id);

    /// Invalidate all cached state for a dictionary. Thread-safe.
    void invalidateDictionaryCache(const std::string & dict_name);

    /// Database context
    ContextMutablePtr global_context;

    /// Dictionary storage
    mutable DB::SharedMutex dict_map_rw_mutex;
    absl::flat_hash_map<std::string, std::shared_ptr<DB::IDictionary>> dictionaries;
    absl::flat_hash_map<std::string, Block> cached_sample_blocks;
    absl::flat_hash_map<std::string, DB::StorageID> cached_source_db_and_table;
    absl::flat_hash_map<std::string, bool> is_simple_key_dict;

    /// Helper objects
    absl::flat_hash_map<std::string, std::shared_ptr<DictionaryKeyManager>> key_managers;
    absl::flat_hash_map<std::string, std::shared_ptr<DictionaryValueConverter>> value_converters;

    LoggerPtr logger;
};

}
