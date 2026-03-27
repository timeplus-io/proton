#include <base/scope_guard.h>
#include <Common/Rocks/RocksDB.h>
/// #include <Common/Rocks/RocksLogger.h>
#include <Common/Rocks/RocksDBTTLCompactionFilter.h>
#include <Common/logger_useful.h>

#include <filesystem>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int ROCKSDB_ERROR;
}

namespace
{
constexpr const char * META_TTL_PREFIX = "__tp_meta_ttl__";

std::string metaTTLKey(const std::string & cf_handle_id)
{
    return fmt::format("{}.{}", META_TTL_PREFIX, cf_handle_id);
}

void writeTTLMeta(rocksdb::DB * db, const std::string & cf_handle_id, int32_t ttl_sec)
{
    rocksdb::Slice ttl{reinterpret_cast<char *>(&ttl_sec), sizeof(ttl_sec)};
    if (auto status = db->Put(rocksdb::WriteOptions{}, metaTTLKey(cf_handle_id), ttl); !status.ok())
        throw Exception(
            ErrorCodes::ROCKSDB_ERROR,
            "Failed to commit ttl_sec={} metadata to for column_family={}, {}",
            ttl_sec,
            cf_handle_id,
            status.ToString());
}

absl::flat_hash_map<std::string_view, int32_t> readTTLMeta(rocksdb::DB * db, const std::vector<rocksdb::ColumnFamilyDescriptor> & cf_descs)
{
    absl::flat_hash_map<std::string_view, int32_t> cf_ttls;

    std::vector<std::string> keys;
    keys.reserve(cf_descs.size());
    std::vector<std::string> values;
    values.reserve(cf_descs.size());

    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(cf_descs.size());

    /// Build keys
    for (const auto & cf_desc : cf_descs)
        keys.emplace_back(metaTTLKey(cf_desc.name));

    for (const auto & key : keys)
        key_slices.emplace_back(key);

    auto statuses = db->MultiGet(rocksdb::ReadOptions{}, key_slices, &values);
    for (size_t i = 0; const auto & status : statuses)
    {
        if (status.ok())
        {
            if (values[i].size() != sizeof(int32_t))
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "TTL value for column_family={} is expected to have {} bytes, but got {}",
                    cf_descs[i].name,
                    sizeof(int32_t),
                    values[i].size());

            int32_t ttl = 0;
            std::memcpy(&ttl, values[i].data(), sizeof(int32_t));

            if (ttl > 0)
                cf_ttls.emplace(cf_descs[i].name, ttl);
        }
        else if (!status.IsNotFound())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR, "Failed to reload TTL for column_family={}, {}", cf_descs[i].name, status.ToString());
        }

        ++i;
    }

    return cf_ttls;
}

}

RocksDB::RocksDB(rocksdb::DB * db_, const std::vector<rocksdb::ColumnFamilyHandle *> & cf_handles_, bool cleanup_, LoggerPtr logger_)
    : db(db_), cleanup(cleanup_), logger(logger_)
{
    chassert(db && logger);
    for (auto * cf_handle : cf_handles_)
        cf_handles.emplace(cf_handle->GetName(), TTLColumnFamilyHandle{.handle = cf_handle, .ttl_sec = 0});
}

RocksDB::RocksDB(
    rocksdb::DB * db_,
    const std::vector<rocksdb::ColumnFamilyHandle *> & cf_handles_,
    const absl::flat_hash_map<std::string_view, int32_t> & cf_ttls_,
    bool cleanup_,
    LoggerPtr logger_)
    : db(db_), cleanup(cleanup_), logger(logger_)
{
    chassert(db && logger);

    for (auto * cf_handle : cf_handles_)
    {
        if (auto it = cf_ttls_.find(cf_handle->GetName()); it != cf_ttls_.end())
            cf_handles.emplace(cf_handle->GetName(), TTLColumnFamilyHandle{.handle = cf_handle, .ttl_sec = it->second});
        else
            cf_handles.emplace(cf_handle->GetName(), TTLColumnFamilyHandle{.handle = cf_handle, .ttl_sec = 0});
    }
}

RocksDB::~RocksDB()
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

RocksDBPtr
RocksDB::createOrLoadIfExists(const rocksdb::Options & options, const std::string & path, int32_t ttl_sec, bool cleanup_, LoggerPtr logger_)
{
    if (!logger_)
        logger_ = getLogger("Rocks");

    {
        LOG_INFO(
            logger_,
            "Init rocks with ttl_sec={} path={} cleanup={} max_background_jobs={} max_write_buffer_number={} enable_blob_files={}",
            ttl_sec,
            path,
            cleanup_,
            options.max_background_jobs,
            options.max_write_buffer_number,
            options.enable_blob_files);
        /// RocksLogger rlogger{rocksdb::InfoLogLevel::INFO_LEVEL, logger};
        /// options.Dump(&rlogger);
    }

    /// Note when we open, we don't apply the ttl_sec compaction filter and we don't apply ttl_sec for default column family for now
    if (std::filesystem::exists(path))
    {
        /// Open existing rocksdb with column families
        std::vector<std::string> column_families;
        if (auto status = rocksdb::DB::ListColumnFamilies(options, path, &column_families); !status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to list column families in path={} : {}", path, status.ToString());

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        cf_descriptors.reserve(column_families.size());
        for (auto & cf : column_families)
            cf_descriptors.emplace_back(std::move(cf), rocksdb::ColumnFamilyOptions(options));

        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
        cf_handles.reserve(cf_descriptors.size());
        rocksdb::DB * db = nullptr;

        if (auto status = rocksdb::DB::Open(options, path, cf_descriptors, &cf_handles, &db); !status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb in path={}: {}", path, status.ToString());

        auto cf_ttls = readTTLMeta(db, cf_descriptors);
        if (cf_ttls.empty())
            return std::make_shared<RocksDB>(db, cf_handles, cleanup_, std::move(logger_));

        auto ttl_v = cf_ttls | std::views::transform([](const auto & cf_ttl) { return fmt::format("{}:{}", cf_ttl.first, cf_ttl.second); });

        LOG_INFO(logger_, "Loaded TTLs: [{}] from path={}", fmt::join(ttl_v, ", "), path);

        /// Closed the column families and db
        {
            for (size_t i = 0; auto * cf_handle : cf_handles)
            {
                if (auto status = db->DestroyColumnFamilyHandle(cf_handle); !status.ok())
                {
                    LOG_ERROR(logger_, "Failed to destroy column family_handle={}: {}", column_families[i], status.ToString());

                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR, "Failed to destroy column family_handle={}: {}", column_families[i], status.ToString());
                }

                ++i;
            }
            cf_handles.clear();

            if (auto status = db->Close(); !status.ok())
                throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to close rocksdb={}: {}", db->GetName(), status.ToString());

            db = nullptr;

            /// Setup compaction filter for column families
            for (auto & cf_desc : cf_descriptors)
            {
                if (auto it = cf_ttls.find(cf_desc.name); it != cf_ttls.end())
                    cf_desc.options.compaction_filter_factory
                        = std::make_shared<DB::RocksDBTTLCompactionFilterFactory>(it->second, cf_desc.name, logger_);
            }

            /// Reopen column families and RocksDB
            if (auto status = rocksdb::DB::Open(options, path, cf_descriptors, &cf_handles, &db); !status.ok())
                throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb in path={}: {}", path, status.ToString());

            return std::make_shared<RocksDB>(db, cf_handles, cf_ttls, cleanup_, std::move(logger_));
        }
    }
    else
    {
        rocksdb::DB * db = nullptr;
        if (auto status = rocksdb::DB::Open(options, path, &db); !status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb in path={}: {}", path, status.ToString());

        return std::make_shared<RocksDB>(db, std::vector<rocksdb::ColumnFamilyHandle *>{}, cleanup_, std::move(logger_));
    }
}

void RocksDB::shutdown(bool cleanup_)
{
    if (shutdown_flag.test_and_set())
        return;

    for (auto & [cf_handle_id, cf_handle] : cf_handles)
    {
        if (auto status = db->DestroyColumnFamilyHandle(cf_handle.handle); !status.ok())
            LOG_ERROR(logger, "Failed to destroy column family_handle={}: {}", cf_handle_id, status.ToString());
    }
    cf_handles.clear();

    if (auto status = db->Close(); !status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to close rocksdb={}: {}", db->GetName(), status.ToString());

    if (cleanup_)
    {
        const auto & path = db->GetName();
        std::filesystem::remove_all(path);
        LOG_INFO(logger, "Cleanup on disk data '{}'", path);
    }

    db.reset();
}

RocksDBColumnFamilyHandlerPtr RocksDB::getOrCreateColumnFamilyHandler(const std::string & cf_handle_id, int32_t ttl_sec)
{
    if (isShutdown())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (cf_handle_id.empty())
        /// ttl is not applied to default column family
        return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this());

    std::lock_guard lock(handles_mutex);
    if (auto it = cf_handles.find(cf_handle_id); it != cf_handles.end())
    {
        if (ttl_sec != it->second.ttl_sec)
            LOG_WARNING(
                logger,
                "The ttl_sec={} passed-in is different than the ttl_sec={} used in the current open column_family={}",
                ttl_sec,
                it->second.ttl_sec,
                cf_handle_id);

        return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this(), it->second.handle, it->second.ttl_sec);
    }

    auto cf_options = rocksdb::ColumnFamilyOptions(db->GetOptions());
    if (ttl_sec > 0)
    {
        cf_options.compaction_filter_factory = std::make_shared<DB::RocksDBTTLCompactionFilterFactory>(ttl_sec, cf_handle_id, logger);
        LOG_INFO(logger, "Creating column_family={} with ttl_sec={}", cf_handle_id, ttl_sec);
    }

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    if (auto status = db->CreateColumnFamily(cf_options, cf_handle_id, &cf_handle); !status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get create column_family={}: {}", cf_handle_id, status.ToString());

    cf_handles.emplace(cf_handle_id, TTLColumnFamilyHandle{.handle = cf_handle, .ttl_sec = ttl_sec});

    /// Commit the TTL to default cf handle, then we can recover the TTLs from it
    writeTTLMeta(db.get(), cf_handle_id, ttl_sec);

    return std::make_shared<RocksDBColumnFamilyHandler>(shared_from_this(), cf_handle, ttl_sec);
}

void RocksDB::destroy(const std::string & cf_handle_id)
{
    if (isShutdown()) [[unlikely]]
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB is already shutdown");

    if (cf_handle_id.empty())
        /// Skipping destroying default cf
        return;

    rocksdb::ColumnFamilyHandle * cf_handle = nullptr;
    {
        std::lock_guard lock(handles_mutex);

        auto it = cf_handles.find(cf_handle_id);
        if (it == cf_handles.end())
            return;

        cf_handle = it->second.handle;
        cf_handles.erase(it);
    }

    if (auto status = db->DropColumnFamily(cf_handle); !status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to drop rocks column_family={}: {}", cf_handle_id, status.ToString());

    LOG_INFO(logger, "Dropped rocks column_family={}", cf_handle_id);
    if (auto status = db->DestroyColumnFamilyHandle(cf_handle); !status.ok())
        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to destroy column_family={}: {}", cf_handle_id, status.ToString());
}

RocksDBColumnFamilyHandler::RocksDBColumnFamilyHandler(RocksDBPtr rocks_, rocksdb::ColumnFamilyHandle * cf_handle_, int32_t ttl_sec_)
    : db(rocks_->db.get()), cf_handle(cf_handle_), ttl_sec(ttl_sec_), rocks(rocks_)
{
    chassert(db);
    if (!cf_handle)
        cf_handle = db->DefaultColumnFamily();

    write_options.disableWAL = rocks_->cleanup;
}

void RocksDBColumnFamilyHandler::destroy()
{
    SCOPE_EXIT({
        db = nullptr;
        cf_handle = nullptr;
        rocks.reset();
    });

    /// Lock the weak_ptr first to ensure the RocksDB is still alive before
    /// accessing the raw db pointer. When ManyAggregatedData is destroyed,
    /// the RocksDB shared_ptrs may be released before the variants that hold
    /// column family handlers, leaving db as a dangling pointer.
    auto rocks_locked = getRocksDB();
    if (!rocks_locked || rocks_locked->isShutdown())
        return;

    /// Does not destroy default column family
    if (cf_handle == db->DefaultColumnFamily())
        return;

    rocks_locked->destroy(cf_handle->GetName());
}

}
