#include <base/scope_guard.h>
#include <Common/Rocks/RocksHandler.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace DB
{
Rocks::Rocks(rocksdb::DB * db_, const std::vector<rocksdb::ColumnFamilyHandle *> & cf_handles_, bool cleanup_, LoggerPtr logger_)
    : db(db_), cleanup(cleanup_), logger(logger_)
{
    chassert(db && logger);
    for (auto * cf_handle : cf_handles_)
        handles.emplace(cf_handle->GetName(), cf_handle);
}

Rocks::~Rocks()
{
    try
    {
        shutdown(cleanup);
    }
    catch (...)
    {
        tryLogCurrentException(logger);
    }
}

RocksPtr Rocks::createOrLoadIfExists(const rocksdb::Options & options, const std::string & path, bool cleanup_, LoggerPtr logger)
{
    if (!logger)
        logger = getLogger("Rocks");

    if (std::filesystem::exists(path))
    {
        /// Open existing rocksdb with column families
        std::vector<std::string> column_families;
        auto status = rocksdb::DB::ListColumnFamilies(options, path, &column_families);
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to list column families: {}", status.ToString());

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        cf_descriptors.reserve(column_families.size());
        for (auto & cf : column_families)
            cf_descriptors.emplace_back(std::move(cf), rocksdb::ColumnFamilyOptions(options));

        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
        cf_handles.reserve(cf_descriptors.size());
        rocksdb::DB * db;
        status = rocksdb::DB::Open(options, path, cf_descriptors, &cf_handles, &db);
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb: {}", status.ToString());

        return std::make_shared<Rocks>(db, cf_handles, cleanup_, logger);
    }
    else
    {
        rocksdb::DB * db;
        auto status = rocksdb::DB::Open(options, path, &db);
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb: {}", status.ToString());

        return std::make_shared<Rocks>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, cleanup_, logger);
    }
}

void Rocks::shutdown(bool cleanup_)
{
    if (shutdown_flag.test_and_set())
        return;

    for (auto & [_, cf_handle] : handles)
    {
        auto status = db->DestroyColumnFamilyHandle(cf_handle);
        if (!status.ok())
            LOG_ERROR(logger, "Failed to destroy column family handle: {}", status.ToString());
    }
    handles.clear();

    auto status = db->Close();
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to close rocksdb: {}", status.ToString());

    if (cleanup_)
    {
        const auto & path = db->GetName();
        std::filesystem::remove_all(path);
        LOG_INFO(logger, "Cleanup on disk data '{}'", path);
    }

    db.reset();
}

RocksHandlerPtr Rocks::getOrCreateHandler(const std::string & handle_id, std::optional<rocksdb::ColumnFamilyOptions> cf_options)
{
    if (isShutdown()) [[unlikely]]
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (handle_id.empty())
        return std::make_shared<RocksHandler>(shared_from_this());

    std::lock_guard lock(handles_mutex);
    auto it = handles.find(handle_id);
    if (it != handles.end())
        return std::make_shared<RocksHandler>(shared_from_this(), it->second);

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    auto status = db->CreateColumnFamily(cf_options ? *cf_options : rocksdb::ColumnFamilyOptions(db->GetOptions()), handle_id, &cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get create column family: {}", status.ToString());

    handles.emplace(handle_id, cf_handle);
    return std::make_shared<RocksHandler>(shared_from_this(), cf_handle);
}

void Rocks::destroy(const std::string & handle_id)
{
    if (isShutdown()) [[unlikely]]
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (handle_id.empty())
        return;

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    {
        std::lock_guard lock(handles_mutex);
        auto it = handles.find(handle_id);
        if (it == handles.end())
            return;

        cf_handle = it->second;
        handles.erase(it);
    }

    auto status = db->DropColumnFamily(cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to drop rocks column family: {}", status.ToString());

    LOG_INFO(logger, "Dropped rocks column family '{}'", cf_handle->GetName());

    status = db->DestroyColumnFamilyHandle(cf_handle);
    if (!status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to destroy column family handle: {}", status.ToString());
}

RocksHandler::RocksHandler(RocksPtr rocks_, rocksdb::ColumnFamilyHandle * cf_handle_)
    : db(rocks_->db.get()), cf_handle(cf_handle_), rocks(rocks_)
{
    chassert(db);
    if (!cf_handle)
        cf_handle = db->DefaultColumnFamily();

    write_options.disableWAL = rocks_->cleanup;
}

void RocksHandler::destroy()
{
    SCOPE_EXIT({
        db = nullptr;
        cf_handle = nullptr;
        rocks.reset();
    });

    /// Does not destroy default column family
    if (cf_handle == db->DefaultColumnFamily())
        return;

    auto rocks_holder = getRocksHolder();
    if (rocks_holder)
        rocks_holder->destroy(cf_handle->GetName());
}

}
