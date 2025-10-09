#pragma once

#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/IDictionary.h>
#include <Common/HybridHashTable/HybridHashTable.h>
#include <Common/SharedMutex.h>

namespace DB
{

struct HybridHashCacheDictionaryConfiguration
{
    std::string spill_dir_path;
    int32_t ttl;
    size_t max_hot_key_count;
    bool cleanup_on_disk_data;
    bool use_async_executor;
    DictionaryLifetime lifetime;
};

class HybridHashCacheDictionary final : public IDictionary
{
public:
    HybridHashCacheDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        HybridHashCacheDictionaryConfiguration configuration_);

    std::string getTypeName() const override { return "HybridHashCache"; }

    /// Hash table metrics
    UInt64 getStorageSize() const override;
    size_t getElementCount() const override;
    size_t getBytesAllocated() const override;
    double getLoadFactor() const override;

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load();
        if (queries == 0)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / queries);
    }

    double getHitRate() const override
    {
        size_t queries = query_count.load();
        if (queries == 0)
            return 0;
        return static_cast<double>(hit_count.load()) / queries;
    }

    bool supportUpdates() const override { return false; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<HybridHashCacheDictionary>(
            getDictionaryID(), dict_struct, getSourceAndUpdateIfNeeded()->clone(), configuration);
    }

    DictionarySourcePtr getSource() const override;

    const DictionaryLifetime & getLifetime() const override { return configuration.lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override { return dict_struct.getAttribute(attribute_name).injective; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }

    ColumnPtr getColumn(
        const ColumnsWithTypeAndName & key_columns_with_types,
        const std::vector<size_t> & key_index_map,
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        DefaultOrFilter default_or_filter) const override;

    Columns getColumns(
        const ColumnsWithTypeAndName & key_columns_with_types,
        const std::vector<size_t> & key_index_map,
        const Strings & attribute_names,
        const DataTypes & attribute_types,
        DefaultsOrFilter defaults_or_filter) const override;

    ColumnUInt8::Ptr
    hasKeys(const ColumnsWithTypeAndName & key_columns_with_types, const std::vector<size_t> & key_index_map) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

    std::exception_ptr getLastException() const override;

    Chunk getByKeys(
        const ColumnsWithTypeAndName & key_columns_with_types,
        PaddedPODArray<UInt8> & out_null_map,
        const Names & result_names,
        const std::vector<size_t> & key_index_map,
        bool add_defaults_if_missing) const override;

    /**
     * Directly insert a value into the dictionary cache
     * @param key The key to insert
     * @param attributes The attribute columns containing the value to insert
     * @param row_num The row number in the attributes columns to use
     * @return True if insertion was successful
     */
    bool Insert(const String & key, const Columns & attributes, size_t row_num);

    /**
     * Directly insert multiple values into the dictionary cache
     * @param keys The keys to insert
     * @param attributes The attribute columns containing the values to insert
     * @return True if insertion was successful
     */
    bool InsertBatch(const std::vector<String> & keys, const Columns & attributes);

private:
    void initHashTable();

    Columns doFetch(
        const Columns & key_columns,
        const DictionaryStorageFetchRequest & fetch_request,
        IColumn::Filter * default_mask,
        bool add_defaults_if_missing) const;

    MutableColumns decodeCacheColumns(
        const std::vector<HybridFindResult> & find_results,
        const DictionaryStorageFetchRequest & fetch_request,
        IColumn::Filter * default_mask,
        bool add_defaults_if_missing) const;

public:
    static std::vector<String> extractKeys(const Columns & key_columns);

private:
    static MutableColumns aggregateColumns(
        const std::vector<String> & keys,
        const DictionaryStorageFetchRequest & request,
        const MutableColumns & cache_columns,
        const std::unordered_set<StringRef> & missing_keys,
        const Columns & attribute_columns_from_source,
        const HashMap<StringRef, size_t> & found_keys_index_from_source,
        IColumn::Filter * default_mask,
        bool add_defaults_if_missing);


    Block getKeysFromSource(const Columns & key_columns, const std::vector<size_t> & requested_rows) const;

    void updateCache(
        const Columns & source_update_attributes,
        const std::vector<String> & found_keys,
        const HashMap<StringRef, size_t> & found_keys_index_from_source,
        const std::unordered_set<StringRef> & missing_keys) const;

    /// Update dictionary source pointer if required and return it. Thread safe.
    /// MultiVersion is not used here because it works with constant pointers.
    /// For some reason almost all methods in IDictionarySource interface are
    /// not constant.
    DictionarySourcePtr getSourceAndUpdateIfNeeded() const
    {
        std::lock_guard lock(source_mutex);
        if (error_count > 0)
        {
            /// Recover after error: we have to clone the source here because
            /// it could keep connections which should be reset after error.
            auto new_source_ptr = source_ptr->clone();
            source_ptr = std::move(new_source_ptr);
        }

        return source_ptr;
    }

    const DictionaryStructure dict_struct;

    /// Dictionary source should be used with mutex
    mutable std::mutex source_mutex;
    mutable DictionarySourcePtr source_ptr TSA_GUARDED_BY(source_mutex);

    const HybridHashCacheDictionaryConfiguration configuration;

    Block sample_block;

    /// Hash table value type
    struct Cell
    {
        bool is_default;
        String value;
        time_t deadline;

        int serialize(WriteBuffer & wb) const
        {
            writeBinary(is_default, wb);
            writeStringBinary(value, wb);
            writeBinary(deadline, wb);
            return ErrorCodes::OK;
        }

        int deserialize(ReadBuffer & rb)
        {
            readBinary(is_default, rb);
            readStringBinary(value, rb);
            readBinary(deadline, rb);
            return ErrorCodes::OK;
        }
    };

    mutable SharedMutex rw_lock;
    std::unique_ptr<HybridHashTable<String>> hash_table;

    mutable std::exception_ptr last_exception;
    mutable std::atomic<size_t> error_count{0};

    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    LoggerPtr logger;
};

}
