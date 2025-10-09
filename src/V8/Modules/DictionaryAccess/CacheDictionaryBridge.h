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
    const Block & getSampleBlock(const std::string & dict_name) const;
    const DB::StorageID & getSourceDbAndTable(const std::string & dict_name) const;
    bool hasDictionary(const std::string & dict_name) const;
    bool isDictionarySimpleKey(const std::string & dict_name) const;

    /// Ensure cached dictionary and its metadata are fresh against current table (detect table recreate/UUID change)
    /// Returns true if dictionary is available and fresh after this call (may reload), false if load failed
    bool ensureFreshDictionary(const std::string & dict_name);

    /// Column value conversion
    v8::Local<v8::Value> columnValueToV8(v8::Isolate * isolate, const IColumn & column, size_t row);
    using ColumnConverter = std::function<v8::Local<v8::Value>(v8::Isolate *, const IColumn &, size_t)>;
    const absl::flat_hash_map<TypeIndex, ColumnConverter> & getColumnConverters();

private:
    CacheDictionaryBridge() = default;

    /// Dictionary and block initialization
    bool initializeSampleBlock(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr);
    bool initializeDictionaryHelpers(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr);

    /// Key management
    std::shared_ptr<DictionaryKeyManager> getKeyManager(const std::string & dict_name);

    /// Value conversion
    std::shared_ptr<DictionaryValueConverter> getValueConverter(const std::string & dict_name);

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
