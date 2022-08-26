#include "MetaStore.h"

#include <NativeLog/Requests/StreamDescription.h>
#include <NativeLog/Rocks/NamespaceKey.h>
#include <NativeLog/Rocks/mapRocksStatus.h>

#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int CANNOT_OPEN_DATABASE;
    const extern int INSERT_KEY_VALUE_FAILED;
    const extern int RESOURCE_ALREADY_EXISTS;
    const extern int RESOURCE_NOT_FOUND;
}
}

namespace nlog
{
namespace
{
    rocksdb::Options getRocksDBOptions()
    {
        rocksdb::Options db_options;
        db_options.create_if_missing = true;
        /// db_options.max_total_wal_size = 1024;
        /// db_options.IncreaseParallelism();
        /// db_options.OptimizeLevelStyleCompaction();

        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        db_options.prefix_extractor.reset(new NamespaceTransform());
        return db_options;
    }
}

MetaStore::MetaStore(const MetaStoreConfig & config_) : config(config_), logger(&Poco::Logger::get("metastore"))
{
    if (!fs::exists(config.meta_dir))
        fs::create_directories(config.meta_dir);

    /// Streams metadata resides in default column family. If necessary create other column family for other metadata
    rocksdb::DB * db = nullptr;
    auto status = rocksdb::DB::Open(getRocksDBOptions(), config.meta_dir, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open metadb {}", status.ToString());

    metadb.reset(db);

    LOG_INFO(logger, "Init metastore in dir={}", config.meta_dir.string());
}

MetaStore::~MetaStore()
{
    shutdown();
}

void MetaStore::startup()
{
}

void MetaStore::shutdown()
{
    if (stopped.test_and_set())
        return;

    auto status = metadb->Close();
    if (!status.ok())
        LOG_ERROR(logger, "Failed to close metadb {}", status.ToString());
}

CreateStreamResponse MetaStore::createStream(const std::string & ns, const CreateStreamRequest & req)
{
    assert(!ns.empty());
    assert(!req.stream.empty());

    auto key = namespaceKey(ns, req.stream);
    auto desc = StreamDescription::from(ns, req);
    auto data{desc.serialize()};
    std::string rvalue;

    std::scoped_lock guard{mlock};

    /// Rocks don't have conditional put
    auto status = metadb->Get(rocksdb::ReadOptions{}, key, &rvalue);
    if (!status.IsNotFound())
    {
        LOG_ERROR(logger, "Failed to create stream={} error={}", key, status.ToString());

        auto response{CreateStreamResponse::from(desc.id, ns, desc.version, 0, 0, req, 0)};
        response.error_code = DB::ErrorCodes::RESOURCE_ALREADY_EXISTS;
        response.error_message = "Stream already exists";
        return response;
    }

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    status = metadb->Put(write_options, key, rocksdb::Slice(data.data(), data.size()));
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to create stream={} error={}", key, status.ToString());
        auto response{CreateStreamResponse::from(desc.id, ns, desc.version, 0, 0, req, 0)};
        response.error_code = DB::ErrorCodes::RESOURCE_ALREADY_EXISTS;
        response.error_message = "Stream already exists";
        return response;
    }

    return CreateStreamResponse::from(desc.id, ns, desc.version, desc.create_timestamp_ms, desc.last_modify_timestamp_ms, req, 0);
}

UpdateStreamResponse MetaStore::updateStream(const std::string & ns, const UpdateStreamRequest & req)
{
    assert(!ns.empty());
    assert(!req.stream.empty());

    auto key = namespaceKey(ns, req.stream);
    std::string rvalue;

    std::scoped_lock guard{mlock};

    /// Rocks don't have conditional put
    auto status = metadb->Get(rocksdb::ReadOptions{}, key, &rvalue);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to find stream={} error={}", key, status.ToString());

        UpdateStreamResponse response{req.id, ns, req.stream};
        response.error_code = DB::ErrorCodes::RESOURCE_NOT_FOUND;
        response.error_message = "Stream not found";
        return response;
    }

    auto desc = StreamDescription::deserialize(rvalue.data(), rvalue.size());

    /// flush settings
    for (const auto & [k, v] : req.flush_settings)
    {
        if (k == "flush_messages")
            desc.flush_messages = v;
        else if (k == "flush_ms")
            desc.flush_ms = v;
    }

    /// retention settings
    for (const auto & [k, v] : req.retention_settings)
    {
        if (k == "retention_bytes")
            desc.retention_bytes = v;
        else if (k == "retention_ms")
            desc.retention_ms = v;
    }
    desc.last_modify_timestamp_ms = DB::UTCMilliseconds::now();

    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    auto data{desc.serialize()};

    status = metadb->Put(write_options, key, rocksdb::Slice(data.data(), data.size()));
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to update stream={} error={}", key, status.ToString());
        UpdateStreamResponse response{req.id, ns, req.stream};
        response.error_code = mapRocksStatus(status);
        response.error_message = "Fail to update stream";
        return response;
    }

    return UpdateStreamResponse::from(req.id, ns, req.stream, desc);
}

DeleteStreamResponse MetaStore::deleteStream(const std::string & ns, const DeleteStreamRequest & req)
{
    assert(!ns.empty());
    assert(!req.stream.empty());

    DeleteStreamResponse response(0);

    auto key = namespaceKey(ns, req.stream);

    /// std::scoped_lock guard{mlock};

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = metadb->Delete(write_options, key);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to delete stream={}", key);
        response.error_message = status.ToString();
        response.error_code = mapRocksStatus(status);
    }

    return response;
}

ListStreamsResponse MetaStore::getStream(const std::string & ns, const ListStreamsRequest & req) const
{
    assert(!ns.empty() && !req.stream.empty());

    std::string rvalue;
    auto key = namespaceKey(ns, req.stream);

    ListStreamsResponse response(0);

    auto status = metadb->Get(rocksdb::ReadOptions{}, key, &rvalue);
    if (status.ok())
    {
        /// assert(iter->key().starts_with(key));
        response.streams.push_back(StreamDescription::deserialize(rvalue.data(), rvalue.size()));
    }
    else
    {
        response.error_code = mapRocksStatus(status);
        response.error_message = status.ToString();
    }

    return response;
}

ListStreamsResponse MetaStore::listAllStreams() const
{
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> iter{metadb->NewIterator(rocksdb::ReadOptions{})};

    iter->SeekToFirst();

    ListStreamsResponse response(0);
    while (iter->Valid())
    {
        const auto & value = iter->value();
        response.streams.push_back(StreamDescription::deserialize(value.data(), value.size()));
        iter->Next();
    }
    return response;
}

ListStreamsResponse MetaStore::listStreamsInNamespace(const std::string & ns) const
{
    rocksdb::ReadOptions options;
    /// options.total_order_seek = true;
    options.auto_prefix_mode = true;
    options.prefix_same_as_start = true;

    std::unique_ptr<rocksdb::Iterator> iter{metadb->NewIterator(options)};

    auto prefix = namespaceKey(ns);
    iter->Seek(prefix);

    ListStreamsResponse response(0);
    while (iter->Valid())
    {
        if (iter->key().starts_with(prefix))
        {
            const auto & value = iter->value();
            response.streams.push_back(StreamDescription::deserialize(value.data(), value.size()));
            iter->Next();
        }
        else
            break;
    }

    return response;
}

ListStreamsResponse MetaStore::listStreams(const std::string & ns, const ListStreamsRequest & req) const
{
    /// std::scoped_lock guard{mlock};

    if (!ns.empty() && !req.stream.empty())
        return getStream(ns, req);

    if (ns.empty())
        return listAllStreams();

    return listStreamsInNamespace(ns);
}

RenameStreamResponse MetaStore::renameStream(const std::string & ns, const RenameStreamRequest & req)
{
    RenameStreamResponse response(req.api_version);
    if (req.new_stream == req.stream)
        return response;

    if (req.stream.empty() || req.new_stream.empty())
    {
        response.error_code = DB::ErrorCodes::BAD_ARGUMENTS;
        response.error_message = "Bad request. Empty stream name";
        return response;
    }

    auto existing_key = namespaceKey(ns, req.stream);
    auto new_key = namespaceKey(ns, req.new_stream);
    assert(existing_key != new_key);

    std::string rvalue;

    std::scoped_lock guard{mlock};

    auto status = metadb->Get(rocksdb::ReadOptions{}, existing_key, &rvalue);
    if (!status.ok())
    {
        response.error_code = mapRocksStatus(status);
        response.error_message = "Failed to rename stream";
        return response;
    }

    auto desc{StreamDescription::deserialize(rvalue.data(), rvalue.size())};
    desc.stream = req.new_stream;
    desc.last_modify_timestamp_ms = DB::UTCMilliseconds::now();
    auto data{desc.serialize()};

    /// We will need do a delete and then do an insert
    rocksdb::WriteBatch batch;

    batch.Delete(existing_key);
    batch.Put(new_key, rocksdb::Slice(data.data(), data.size()));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    status = metadb->Write(write_options, &batch);
    if (status.ok())
        return response;

    LOG_ERROR(logger, "Failed to rename stream, error={}", status.ToString());

    response.error_code = mapRocksStatus(status);
    response.error_message = "Failed to rename stream";
    return response;
}

}
