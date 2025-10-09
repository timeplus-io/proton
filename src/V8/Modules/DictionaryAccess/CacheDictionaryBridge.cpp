#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Storages/IStorage.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>

namespace DB::V8
{
namespace
{
std::pair<std::string, std::string> extractTimeplusDbAndTableFromDict(const std::string & source_info)
{
    if (!source_info.starts_with("timeplus: "))
        return {"", ""};

    /// Extract after "timeplus: " prefix
    size_t start_pos = std::string("timeplus: ").length();

    size_t dot_pos = source_info.find('.', start_pos);
    if (dot_pos == std::string::npos)
        return {"", ""};

    std::string db = source_info.substr(start_pos, dot_pos - start_pos);

    /// Find end of table name (before ", where: " if it exists)
    size_t where_pos = source_info.find(", where: ", dot_pos);

    std::string table;
    if (where_pos != std::string::npos)
        table = source_info.substr(dot_pos + 1, where_pos - (dot_pos + 1));
    else
        table = source_info.substr(dot_pos + 1);

    return {db, table};
}
}

CacheDictionaryBridge & CacheDictionaryBridge::instance()
{
    static CacheDictionaryBridge inst;
    return inst;
}

void CacheDictionaryBridge::initialize(ContextMutablePtr context)
{
    global_context = context;
    logger = getLogger("CacheDictionaryBridge");
    LOG_INFO(logger, "CacheDictionaryBridge initialized");
}

bool CacheDictionaryBridge::loadDictionary(const std::string & dict_name)
{
    std::unique_lock lock(dict_map_rw_mutex);

    try
    {
        if (dictionaries.find(dict_name) != dictionaries.end())
            return true;

        const ExternalDictionariesLoader & external_loader = global_context->getExternalDictionariesLoader();
        auto load_result = external_loader.tryGetDictionary(dict_name, global_context);

        if (!load_result)
        {
            LOG_ERROR(logger, "Dictionary '{}' not found", dict_name);
            return false;
        }

        auto dict_ptr = std::const_pointer_cast<DB::IDictionary>(std::static_pointer_cast<const DB::IDictionary>(load_result));

        if (!dict_ptr)
        {
            LOG_ERROR(logger, "Dictionary is not a valid IDictionary");
            return false;
        }

        bool is_simple_key = false;
        if (auto simple_direct_dict = std::dynamic_pointer_cast<DB::DirectDictionary<DictionaryKeyType::Simple>>(dict_ptr))
        {
            is_simple_key = true;
            LOG_INFO(logger, "Dictionary '{}' is a Direct Dictionary with simple key", dict_name);
        }
        else if (auto complex_direct_dict = std::dynamic_pointer_cast<DB::DirectDictionary<DictionaryKeyType::Complex>>(dict_ptr))
        {
            is_simple_key = false;
            LOG_INFO(logger, "Dictionary '{}' is a Direct Dictionary with complex key", dict_name);
        }
        else
        {
            LOG_ERROR(logger, "Dictionary '{}' is not a DirectDictionary", dict_name);
            return false;
        }

        dictionaries[dict_name] = dict_ptr;
        is_simple_key_dict[dict_name] = is_simple_key;

        LOG_DEBUG(logger, "Dictionary '{}' loaded successfully", dict_name);
        return initializeSampleBlock(dict_name, dict_ptr) && initializeDictionaryHelpers(dict_name, dict_ptr);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to load dictionary '{}': {}", dict_name, e.displayText());
        return false;
    }
}

void CacheDictionaryBridge::invalidateDictionaryCache(const std::string & dict_name)
{
    std::unique_lock lock(dict_map_rw_mutex);
    dictionaries.erase(dict_name);
    cached_sample_blocks.erase(dict_name);
    cached_source_db_and_table.erase(dict_name);
    is_simple_key_dict.erase(dict_name);
    key_managers.erase(dict_name);
    value_converters.erase(dict_name);
}

bool CacheDictionaryBridge::ensureFreshDictionary(const std::string & dict_name)
{
    /// Quick path: if not loaded, load it
    if (!hasDictionary(dict_name))
        return loadDictionary(dict_name);

    try
    {
        /// Capture cached table id under shared lock
        DB::StorageID cached_id = DB::StorageID::createEmpty();
        bool missing_cached_id = false;
        {
            std::shared_lock rlock(dict_map_rw_mutex);
            auto it = cached_source_db_and_table.find(dict_name);
            if (it == cached_source_db_and_table.end())
                missing_cached_id = true;
            else
                cached_id = it->second;
        }
        if (missing_cached_id)
        {
            /// Incomplete cache, force reload
            invalidateDictionaryCache(dict_name);
            return loadDictionary(dict_name);
        }

        /// Fetch current table by db.table and compare StorageID (UUID and/or name)
        StoragePtr current_table = DatabaseCatalog::instance().tryGetTable({cached_id.database_name, cached_id.table_name}, global_context);
        if (!current_table)
        {
            LOG_WARNING(logger, "Source table for dictionary '{}' not found anymore; invalidating cache", dict_name);
            invalidateDictionaryCache(dict_name);
            return loadDictionary(dict_name);
        }

        const DB::StorageID & current_id = current_table->getStorageID();
        /// Identity check: backend is expected to have UUID; compare UUIDs when available, otherwise force reload
        if (current_id.hasUUID() && cached_id.hasUUID())
        {
            if (current_id.uuid != cached_id.uuid)
            {
                LOG_INFO(
                    logger,
                    "Detected table identity change for dictionary '{}': {}.{} -> {}.{}; reloading",
                    dict_name,
                    cached_id.getDatabaseName(),
                    cached_id.getTableName(),
                    current_id.getDatabaseName(),
                    current_id.getTableName());
                invalidateDictionaryCache(dict_name);
                return loadDictionary(dict_name);
            }
        }
        else
        {
            LOG_WARNING(logger, "UUID missing for dictionary '{}' identity check; forcing reload", dict_name);
            invalidateDictionaryCache(dict_name);
            return loadDictionary(dict_name);
        }

        /// Skip metadata version refresh, user shall recreate table instead use ALTER ADD COLUMN here.

        return true;
    }
    catch (const DB::Exception & e)
    {
        LOG_WARNING(logger, "Error ensuring freshness for dictionary '{}': {}. Forcing reload", dict_name, e.displayText());
        invalidateDictionaryCache(dict_name);
        return loadDictionary(dict_name);
    }
    catch (...)
    {
        LOG_WARNING(logger, "Unknown error ensuring freshness for dictionary '{}'. Forcing reload", dict_name);
        invalidateDictionaryCache(dict_name);
        return loadDictionary(dict_name);
    }
}

bool CacheDictionaryBridge::initializeSampleBlock(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr)
{
    try
    {
        std::string source_db, source_table;
        try
        {
            auto source_ptr = dict_ptr->getSource();
            if (source_ptr)
            {
                std::string source_info = source_ptr->toString();
                LOG_INFO(logger, "Dictionary source info: {}", source_info);

                std::tie(source_db, source_table) = extractTimeplusDbAndTableFromDict(source_info);
            }
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(logger, "Failed to get source info from dictionary: {}", e.displayText());
        }
        catch (...)
        {
            LOG_WARNING(logger, "Unknown error getting source info from dictionary");
        }

        LOG_INFO(logger, "Using source table for dictionary '{}': {}.{}", dict_name, source_db, source_table);

        StoragePtr table = DatabaseCatalog::instance().getTable({std::move(source_db), std::move(source_table)}, global_context);
        if (!table)
        {
            LOG_ERROR(logger, "Could not find any table for dictionary '{}'", dict_name);
            return false;
        }
        cached_source_db_and_table.emplace(dict_name, table->getStorageID());

        auto metadata_snapshot = table->getInMemoryMetadataPtr();
        cached_sample_blocks[dict_name] = metadata_snapshot->getSampleBlockNonMaterialized(/*skip_tp_sn=*/true);

        LOG_INFO(
            logger, "Sample block cached from source table for '{}' with {} columns", dict_name, cached_sample_blocks[dict_name].columns());
        LOG_TRACE(logger, "Sample block structure: {}", cached_sample_blocks[dict_name].dumpStructure());
        return true;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to cache sample block for '{}': {}", dict_name, e.displayText());
        return false;
    }
}

bool CacheDictionaryBridge::initializeDictionaryHelpers(const std::string & dict_name, const std::shared_ptr<DB::IDictionary> & dict_ptr)
{
    try
    {
        const auto & dict_struct = dict_ptr->getStructure();
        key_managers[dict_name] = std::make_shared<DictionaryKeyManager>(dict_struct);
        value_converters[dict_name] = std::make_shared<DictionaryValueConverter>(dict_struct);

        LOG_INFO(logger, "Dictionary helpers initialized for '{}'", dict_name);
        return true;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to initialize dictionary helpers for '{}': {}", dict_name, e.displayText());
        return false;
    }
}

std::shared_ptr<DB::IDictionary> CacheDictionaryBridge::getDictionary(const std::string & dict_name) const
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = dictionaries.find(dict_name);
    if (it == dictionaries.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Dictionary '{}' not cached", dict_name);
    return it->second;
}

const Block & CacheDictionaryBridge::getSampleBlock(const std::string & dict_name) const
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = cached_sample_blocks.find(dict_name);
    if (it == cached_sample_blocks.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Sample block for dictionary '{}' not cached", dict_name);
    return it->second;
}

const DB::StorageID & CacheDictionaryBridge::getSourceDbAndTable(const std::string & dict_name) const
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = cached_source_db_and_table.find(dict_name);
    if (it == cached_source_db_and_table.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Source DB and table for dictionary '{}' not cached", dict_name);
    return it->second;
}

bool CacheDictionaryBridge::hasDictionary(const std::string & dict_name) const
{
    std::shared_lock lock(dict_map_rw_mutex);
    return dictionaries.find(dict_name) != dictionaries.end();
}

bool CacheDictionaryBridge::isDictionarySimpleKey(const std::string & dict_name) const
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = is_simple_key_dict.find(dict_name);
    if (it == is_simple_key_dict.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Dictionary type for '{}' not cached", dict_name);
    return it->second;
}

std::shared_ptr<DictionaryKeyManager> CacheDictionaryBridge::getKeyManager(const std::string & dict_name)
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = key_managers.find(dict_name);
    if (it == key_managers.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Key manager for dictionary '{}' not initialized", dict_name);
    return it->second;
}

std::shared_ptr<DictionaryValueConverter> CacheDictionaryBridge::getValueConverter(const std::string & dict_name)
{
    std::shared_lock lock(dict_map_rw_mutex);
    auto it = value_converters.find(dict_name);
    if (it == value_converters.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Value converter for dictionary '{}' not initialized", dict_name);
    return it->second;
}

}
