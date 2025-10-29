#include <Dictionaries/HybridHashCacheDictionary.h>

#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Processors/Sources/NullSource.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfilingScopedRWLock.h>

#include <thread>

namespace ProfileEvents
{
extern const Event DictCacheLockReadNs;
extern const Event DictCacheLockWriteNs;
}

namespace DB
{

namespace ErrorCodes
{
extern const int NETWORK_ERROR;
}

namespace
{
/// Copied from DictionarySourceFactory.cpp but has 0 rows.
Block createSampleBlock(const DictionaryStructure & dict_struct)
{
    Block block;

    if (dict_struct.id)
    {
        block.insert(ColumnWithTypeAndName{ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), dict_struct.id->name});
    }
    else if (dict_struct.key)
    {
        for (const auto & attribute : *dict_struct.key)
        {
            auto column = attribute.type->createColumn();
            block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
        }
    }

    if (dict_struct.range_min)
    {
        for (const auto & attribute : {dict_struct.range_min, dict_struct.range_max})
        {
            const auto & type = std::make_shared<DataTypeNullable>(attribute->type);
            auto column = type->createColumn();
            block.insert(ColumnWithTypeAndName{std::move(column), type, attribute->name});
        }
    }

    for (const auto & attribute : dict_struct.attributes)
    {
        auto column = attribute.type->createColumn();
        block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
    }

    return block;
}
} /// namespace

HybridHashCacheDictionary::HybridHashCacheDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    HybridHashCacheDictionaryConfiguration configuration_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(std::move(configuration_))
    , sample_block(createSampleBlock(dict_struct))
    , logger(getLogger("HybridHashCacheDictionary"))
{
    initHashTable();
}

void HybridHashCacheDictionary::initHashTable()
{
    HybridHashTableConfig config;
    config.spill_dir_path = configuration.spill_dir_path;
    config.ttl = configuration.ttl;
    config.max_hot_key_count = configuration.max_hot_key_count;
    config.cleanup_on_disk_data = configuration.cleanup_on_disk_data;

    config.value_object_size = sizeof(Cell);
    config.align_value_object_size = alignof(Cell);
    config.value_constructor = [](void * data) { new (data) Cell; };
    config.value_destructor = []([[maybe_unused]] void * data) {};
    config.value_serializer = [](const void * value, WriteBuffer & wb) { return static_cast<const Cell *>(value)->serialize(wb); };
    config.value_deserializer = [](void * value, ReadBuffer & rb) { return static_cast<Cell *>(value)->deserialize(rb); };

    hash_table = std::make_unique<HybridHashTable<String>>(
        std::move(config),
        [](const String & key, WriteBuffer & wb) {
            writeStringBinary(key, wb);
            return ErrorCodes::OK;
        },
        [](String & key, ReadBuffer & rb) {
            readStringBinary(key, rb);
            return ErrorCodes::OK;
        },
        logger);
}

std::vector<String> HybridHashCacheDictionary::extractKeys(const Columns & key_columns)
{
    chassert(!key_columns.empty());

    const auto num_keys = key_columns[0]->size();

    std::vector<String> keys(num_keys);
    for (size_t i = 0; i < num_keys; ++i)
    {
        WriteBufferFromString wb(keys[i]);
        for (const auto & column : key_columns)
            column->serializeValueIntoBuffer(i, wb);
    }

    return keys;
}

MutableColumns HybridHashCacheDictionary::aggregateColumns(
    const std::vector<String> & keys,
    const DictionaryStorageFetchRequest & request,
    const MutableColumns & cache_columns,
    const std::unordered_set<StringRef> & missing_keys,
    const Columns & attribute_columns_from_source,
    const HashMap<StringRef, size_t> & found_keys_index_from_source,
    IColumn::Filter * default_mask,
    bool add_defaults_if_missing)
{
    const auto num_keys = keys.size();
    const auto dict_attributes_size = request.attributesSize();

    auto aggregated_columns = request.makeAttributesResultColumns();

    /// cache_columns may not include default rows so need the separate index to track
    size_t next_cache_row = 0;

    for (size_t i = 0; i < num_keys; ++i)
    {
        bool key_in_cache = !missing_keys.contains(keys[i]);
        std::optional<size_t> key_index_from_source;
        if (!key_in_cache)
        {
            const auto * index_cell = found_keys_index_from_source.find(keys[i]);
            if (index_cell != nullptr)
            {
                key_index_from_source = index_cell->getMapped();
                if (default_mask != nullptr)
                    (*default_mask)[i] = 1;
            }
        }

        for (size_t attr_idx = 0; attr_idx < dict_attributes_size; ++attr_idx)
        {
            if (!request.shouldFillResultColumnWithIndex(attr_idx))
                continue;

            auto & column = *aggregated_columns[attr_idx];

            if (key_in_cache)
                column.insertFrom(*cache_columns[attr_idx], next_cache_row);
            else if (key_index_from_source.has_value())
                column.insertFrom(*attribute_columns_from_source[attr_idx], key_index_from_source.value());
            else if (add_defaults_if_missing) /// get default row from cache_columns
                column.insertFrom(*cache_columns[attr_idx], next_cache_row);
        }

        if (add_defaults_if_missing || key_in_cache)
            ++next_cache_row;
    }

    return aggregated_columns;
}

Block HybridHashCacheDictionary::getKeysFromSource(const Columns & key_columns, const std::vector<size_t> & requested_rows) const
{
    Stopwatch watch;

    Block merged_block;

    constexpr int MAX_RETRIES = 10;
    constexpr int RETRY_WAIT_SEC = 1;
    for (int retries = 0; retries < MAX_RETRIES; ++retries)
    {
        try
        {
            auto current_source_ptr = getSourceAndUpdateIfNeeded();
            auto pipeline = current_source_ptr->loadKeys(key_columns, requested_rows);
            DictionaryPipelineExecutor executor(pipeline, configuration.use_async_executor);

            Block block;
            while (executor.pull(block))
            {
                if (!merged_block)
                {
                    merged_block.swap(block);
                    continue;
                }

                if (block.rows() > 0)
                    merged_block.concat(block);
            }

            break;
        }
        catch (const Exception & ex)
        {
            /// Only retry for network error
            if (ex.code() != ErrorCodes::NETWORK_ERROR || retries == MAX_RETRIES - 1)
                throw;

            merged_block.clear();
            std::this_thread::sleep_for(std::chrono::seconds(RETRY_WAIT_SEC));
        }
    }

    auto merged_rows = merged_block.rows();

    LOG_INFO(
        logger,
        "Took {}ms to fetch {} rows from remote source, requested num_keys={}, total_keys={}",
        watch.elapsedMilliseconds(),
        merged_rows,
        requested_rows.size(),
        key_columns.size());

    return merged_block;
}

void HybridHashCacheDictionary::updateCache(
    const Columns & source_update_attributes,
    const std::vector<String> & found_keys,
    const HashMap<StringRef, size_t> & found_keys_index_from_source,
    const std::unordered_set<StringRef> & missing_keys) const
{
    const auto now = std::chrono::system_clock::now();
    const auto deadline = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(configuration.ttl));

    Stopwatch watch;

    ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    /// Update found keys
    auto emplace_results = hash_table->emplaceKeys(found_keys);
    if (emplace_results.hasError())
    {
        last_exception = std::make_exception_ptr(Exception(emplace_results.errcode, "Failed to update hybrid hash cache"));
        return;
    }

    size_t found_keys_size = found_keys.size();
    chassert(emplace_results.results.size() == found_keys_size);

    for (size_t i = 0; i < found_keys_size; ++i)
    {
        const auto * index_cell = found_keys_index_from_source.find(found_keys[i]);
        if (unlikely(index_cell == nullptr))
            continue; /// for safety

        const auto source_index = index_cell->getMapped();
        auto * cell = static_cast<Cell *>(emplace_results.results[i].getMutableMapped());
        {
            WriteBufferFromString wb(cell->value);
            for (const auto & column : source_update_attributes)
                column->serializeValueIntoBuffer(source_index, wb);
        }
        cell->is_default = false;
        cell->deadline = deadline;
    }

    /// NOTE: found_keys may have duplicated keys. Use found_keys_index_from_source to check if any not found keys
    if (missing_keys.size() == found_keys_index_from_source.size())
        return;

    chassert(missing_keys.size() > found_keys_index_from_source.size());

    /// Update not found keys (default keys)
    size_t not_found_keys_size = missing_keys.size() - found_keys_index_from_source.size();
    std::vector<String> not_found_keys;
    not_found_keys.reserve(not_found_keys_size);
    for (const auto & key : missing_keys)
    {
        if (found_keys_index_from_source.find(key) == nullptr)
            not_found_keys.push_back(key.toString());
    }

    chassert(not_found_keys.size() == not_found_keys_size);

    emplace_results = hash_table->emplaceKeys(not_found_keys);
    if (emplace_results.hasError())
    {
        last_exception = std::make_exception_ptr(Exception(emplace_results.errcode, "Failed to update hybrid hash cache"));
        return;
    }

    chassert(emplace_results.results.size() == not_found_keys_size);

    for (size_t i = 0; i < not_found_keys_size; ++i)
    {
        auto * cell = static_cast<Cell *>(emplace_results.results[i].getMutableMapped());
        cell->value.clear();
        cell->is_default = true;
        cell->deadline = deadline;
    }

    LOG_DEBUG(
        logger, "Took {}ms to insert {} found keys and {} default keys", watch.elapsedMilliseconds(), found_keys_size, not_found_keys_size);
}

UInt64 HybridHashCacheDictionary::getStorageSize() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return hash_table->getDiskSize();
}

size_t HybridHashCacheDictionary::getElementCount() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return hash_table->approximateCount();
}

size_t HybridHashCacheDictionary::getBytesAllocated() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return hash_table->getBufferSizeInBytes();
}

double HybridHashCacheDictionary::getLoadFactor() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return 0;
}

std::exception_ptr HybridHashCacheDictionary::getLastException() const
{
    const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};
    return last_exception;
}

DictionarySourcePtr HybridHashCacheDictionary::getSource() const
{
    std::lock_guard lock(source_mutex);
    return source_ptr;
}

ColumnPtr HybridHashCacheDictionary::getColumn(
    const ColumnsWithTypeAndName & key_columns_with_types,
    const std::vector<size_t> & key_index_map,
    const std::string & attribute_name,
    const DataTypePtr & attribute_type,
    DefaultOrFilter default_or_filter) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(default_or_filter);
    chassert(is_short_circuit || std::holds_alternative<RefDefault>(default_or_filter));

    if (is_short_circuit)
    {
        IColumn::Filter & default_mask = std::get<RefFilter>(default_or_filter).get();
        return getColumns(key_columns_with_types, key_index_map, {attribute_name}, {attribute_type}, default_mask).front();
    }

    const ColumnPtr & default_values_column = std::get<RefDefault>(default_or_filter).get();
    Columns columns{default_values_column};
    return getColumns(key_columns_with_types, key_index_map, {attribute_name}, {attribute_type}, columns).front();
}

Columns HybridHashCacheDictionary::getColumns(
    const ColumnsWithTypeAndName & key_columns_with_types,
    const std::vector<size_t> & key_index_map,
    const Strings & attribute_names,
    const DataTypes & attribute_types,
    DefaultsOrFilter defaults_or_filter) const
{
    bool is_short_circuit = std::holds_alternative<RefFilter>(defaults_or_filter);
    assert(is_short_circuit || std::holds_alternative<RefDefaults>(defaults_or_filter));

    dict_struct.validateKeyTypes(key_columns_with_types, key_index_map);

    auto key_columns = getOrderedKeyColumns(key_columns_with_types, key_index_map);

    DictionaryStorageFetchRequest fetch_request(
        dict_struct, attribute_names, attribute_types, is_short_circuit ? nullptr : &std::get<RefDefaults>(defaults_or_filter).get());

    IColumn::Filter * default_mask = nullptr;
    if (is_short_circuit)
        default_mask = &std::get<RefFilter>(defaults_or_filter).get();

    return doFetch(key_columns, fetch_request, default_mask, true);
}

ColumnUInt8::Ptr
HybridHashCacheDictionary::hasKeys(const ColumnsWithTypeAndName & key_columns_with_types, const std::vector<size_t> & key_index_map) const
{
    (void)key_columns_with_types;
    (void)key_index_map;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "hasKeys is not implemented");
}

Pipe HybridHashCacheDictionary::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    (void)column_names;
    (void)max_block_size;
    (void)num_streams;
    return Pipe(std::make_shared<NullSource>(sample_block));
}

Chunk HybridHashCacheDictionary::getByKeys(
    const ColumnsWithTypeAndName & key_columns_with_types,
    PaddedPODArray<UInt8> & out_null_map,
    const Names & result_names,
    const std::vector<size_t> & key_index_map,
    bool add_defaults_if_missing) const
{
    if (key_columns_with_types.empty())
        return Chunk(getSampleBlock(result_names).cloneEmpty().getColumns(), 0);

    Names attribute_names;
    DataTypes result_types;
    if (!result_names.empty())
    {
        for (const auto & attr_name : result_names)
        {
            if (!dict_struct.hasAttribute(attr_name))
                continue; /// skip keys
            const auto & attr = dict_struct.getAttribute(attr_name);
            attribute_names.emplace_back(attr.name);
            result_types.emplace_back(attr.type);
        }
    }
    else
    {
        /// If result_names is empty, then use all attributes from dictionary_structure
        for (const auto & attr : dict_struct.attributes)
        {
            attribute_names.emplace_back(attr.name);
            result_types.emplace_back(attr.type);
        }
    }

    dict_struct.validateKeyTypes(key_columns_with_types, key_index_map);

    auto key_columns = getOrderedKeyColumns(key_columns_with_types, key_index_map);

    DictionaryStorageFetchRequest fetch_request(dict_struct, attribute_names, result_types, nullptr);

    auto result_columns = doFetch(key_columns, fetch_request, &out_null_map, add_defaults_if_missing);

    /// Result block should consist of key columns and then attributes
    Columns key_and_attr_columns;
    key_and_attr_columns.reserve(key_columns_with_types.size() + result_columns.size());
    /// Result block should consist of key columns and then attributes
    for (auto idx : key_index_map)
    {
        const auto & col = key_columns_with_types[idx].column;
        ColumnPtr filtered_key_col;
        if (add_defaults_if_missing)
            filtered_key_col = JoinCommon::filterWithBlanks(col, out_null_map);
        else
            filtered_key_col = col->filter(out_null_map, -1);
        key_and_attr_columns.push_back(std::move(filtered_key_col));
    }
    key_and_attr_columns.insert(key_and_attr_columns.end(), result_columns.begin(), result_columns.end());

    size_t num_rows = key_and_attr_columns[0]->size();
    return Chunk(std::move(key_and_attr_columns), num_rows);
}

Columns HybridHashCacheDictionary::doFetch(
    const Columns & key_columns,
    const DictionaryStorageFetchRequest & fetch_request,
    IColumn::Filter * default_mask,
    bool add_defaults_if_missing) const
{
    MutableColumns result_columns;

    Columns source_update_keys;
    Columns source_update_attributes;

    /// keys hold the String reference in missing_keys
    const auto keys = extractKeys(key_columns);
    const auto num_keys = keys.size();
    std::unordered_set<StringRef> missing_keys;

    /// found_keys hold the String reference in found_keys_index_from_source
    std::vector<String> found_keys;
    HashMap<StringRef, size_t> found_keys_index_from_source;

    std::vector<size_t> requested_rows;

    const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    if (default_mask != nullptr)
        default_mask->resize(num_keys);

    {
        /// Hold write lock of hybrid hash table as it is changed in find keys
        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

        auto find_results = hash_table->findKeys(keys);
        if (find_results.hasError())
            throw Exception(find_results.errcode, "Failed to find keys in hybrid hash table");

        chassert(find_results.results.size() == num_keys);

        for (size_t i = 0; i < num_keys; ++i)
        {
            auto & find_result = find_results.results[i];
            if (!find_result.isFound() || static_cast<const Cell *>(find_result.getMapped())->deadline < now)
            {
                const auto & key = keys[i];

                /// Key was expired, remove it from the hash table
                /// FIXME: Remove batch missing keys
                if (find_result.isFound())
                    hash_table->removeKey(key);

                if (!missing_keys.contains(key))
                {
                    missing_keys.emplace(key);
                    requested_rows.push_back(i);
                }
            }
        }

        result_columns = decodeCacheColumns(find_results.results, fetch_request, default_mask, add_defaults_if_missing);
    }

    if (requested_rows.empty())
        return fetch_request.filterRequestedColumns(result_columns);

    /// Fetch from source and split by key columns and attribute columns
    auto source_update_block = getKeysFromSource(key_columns, requested_rows);
    if (source_update_block.rows() > 0)
    {
        /// Split into keys columns and attribute columns
        source_update_keys.reserve(key_columns.size());
        source_update_attributes.reserve(source_update_block.columns() - key_columns.size());
        auto source_columns = source_update_block.mutateColumns();
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            if (i < key_columns.size())
                source_update_keys.emplace_back(std::move(source_columns[i]));
            else
                source_update_attributes.emplace_back(std::move(source_columns[i]));
        }

        found_keys = extractKeys(source_update_keys);
        found_keys_index_from_source.reserve(found_keys.size());
        for (size_t i = 0; i < found_keys.size(); ++i)
            found_keys_index_from_source[found_keys[i]] = i;
    }

    result_columns = aggregateColumns(
        keys,
        fetch_request,
        result_columns,
        missing_keys,
        source_update_attributes,
        found_keys_index_from_source,
        default_mask,
        add_defaults_if_missing);

    /// Update cache by source data
    if (!missing_keys.empty())
        updateCache(source_update_attributes, found_keys, found_keys_index_from_source, missing_keys);

    return fetch_request.filterRequestedColumns(result_columns);
}

MutableColumns HybridHashCacheDictionary::decodeCacheColumns(
    const std::vector<HybridFindResult> & find_results,
    const DictionaryStorageFetchRequest & fetch_request,
    IColumn::Filter * default_mask,
    bool add_defaults_if_missing) const
{
    chassert(default_mask == nullptr || default_mask->size() == find_results.size());

    const auto num_keys = find_results.size();
    const auto dict_attributes_size = fetch_request.attributesSize();

    auto result_columns = fetch_request.makeAttributesResultColumns();

    for (size_t key_idx = 0; key_idx < num_keys; ++key_idx)
    {
        const auto * cell = static_cast<const Cell *>(find_results[key_idx].getMapped());
        bool is_default = !find_results[key_idx].isFound() || cell == nullptr || cell->is_default;
        const char * pos = is_default ? nullptr : cell->value.data();

        if (default_mask != nullptr)
            (*default_mask)[key_idx] = is_default ? 0 : 1;

        for (size_t attr_idx = 0; attr_idx < dict_attributes_size; ++attr_idx)
        {
            auto & column = *result_columns[attr_idx];
            if (!fetch_request.shouldFillResultColumnWithIndex(attr_idx))
            {
                if (pos != nullptr)
                    pos = column.skipSerializedInArena(pos);
                continue;
            }

            if (!is_default)
            {
                pos = column.deserializeAndInsertFromArena(pos);
            }
            else if (add_defaults_if_missing)
            {
                if (default_mask != nullptr)
                {
                    column.insertDefault();
                }
                else
                {
                    const auto & default_value_provider = fetch_request.defaultValueProviderAtIndex(attr_idx);
                    column.insert(default_value_provider.getDefaultValue(key_idx));
                }
            }
        }
    }

    return result_columns;
}

bool HybridHashCacheDictionary::Insert(const String & key, const Columns & attributes, size_t row_num)
{
    /// Validate that we have the correct number of attribute columns
    if (attributes.size() != dict_struct.attributes.size())
    {
        LOG_ERROR(
            logger,
            "Wrong number of attribute columns passed to hybrid hash cache dictionary(insert). Expected {} attribute columns, got {}",
            dict_struct.attributes.size(),
            attributes.size());
        return false;
    }

    const auto now = std::chrono::system_clock::now();
    const auto deadline = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(configuration.ttl));

    ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    auto emplace_result = hash_table->emplaceKey(key, false);
    if (emplace_result.hasError())
    {
        last_exception = std::make_exception_ptr(Exception(emplace_result.errorCode(), "Failed to insert key into hybrid hash cache"));
        return false;
    }

    auto * cell = static_cast<Cell *>(emplace_result.getMutableMapped());

    /// Serialize attribute values into the cell
    {
        WriteBufferFromString wb(cell->value);
        for (const auto & column : attributes)
            column->serializeValueIntoBuffer(row_num, wb);
    }

    cell->is_default = false;
    cell->deadline = deadline;

    return true;
}

bool HybridHashCacheDictionary::InsertBatch(const std::vector<String> & keys, const Columns & attributes)
{
    if (keys.empty())
        return true;

    /// Validate that we have the correct number of attribute columns
    if (attributes.size() != dict_struct.attributes.size())
    {
        LOG_ERROR(
            logger,
            "Wrong number of attribute columns passed to hybrid hash cache dictionary(batch insert). Expected {} attribute columns, got {}",
            dict_struct.attributes.size(),
            attributes.size());
        return false;
    }

    /// Validate that all attribute columns have exactly the same number of rows as keys
    for (const auto & column : attributes)
    {
        if (column->size() != keys.size())
        {
            LOG_ERROR(
                logger, "Attribute column has {} rows, but expected exactly {} rows to match the number of keys", column->size(), keys.size());
            return false;
        }
    }

    const auto now = std::chrono::system_clock::now();
    const auto deadline = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(configuration.ttl));

    ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};

    auto emplace_results = hash_table->emplaceKeys(keys);
    if (emplace_results.hasError())
    {
        last_exception = std::make_exception_ptr(Exception(emplace_results.errorCode(), "Failed to insert keys into hybrid hash cache"));
        return false;
    }

    chassert(emplace_results.results.size() == keys.size());

    for (size_t i = 0; i < keys.size(); ++i)
    {
        auto * cell = static_cast<Cell *>(emplace_results.results[i].getMutableMapped());

        /// Serialize attribute values into the cell
        {
            WriteBufferFromString wb(cell->value);
            for (const auto & column : attributes)
                column->serializeValueIntoBuffer(i, wb);
        }

        cell->is_default = false;
        cell->deadline = deadline;
    }

    return true;
}
}
