#include "MetaStore.h"

#include <NativeLog/Requests/StreamDescription.h>
#include <NativeLog/Rocks/NamespaceKey.h>
#include <NativeLog/Rocks/mapRocksStatus.h>

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

    auto status = metadb->Get(rocksdb::ReadOptions{}, key, &rvalue);
    if (!status.IsNotFound())
        /// stream already exists
        throw DB::Exception(DB::ErrorCodes::RESOURCE_ALREADY_EXISTS, "Stream {} in namespace {} already exists", req.stream, ns);

    status = metadb->Put(rocksdb::WriteOptions{}, key, rocksdb::Slice(data.data(), data.size()));
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to create stream={} error={}", key, status.ToString());
        throw DB::Exception(DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Failed to create stream={}, error={}", req.stream, status.ToString());
    }

    return CreateStreamResponse::from(desc.id, ns, desc.version, desc.create_timestamp_ms, desc.last_modify_timestamp_ms, req, 0);
}

DeleteStreamResponse MetaStore::deleteStream(const std::string & ns, const DeleteStreamRequest & req)
{
    assert(!ns.empty());
    assert(!req.stream.empty());

    DeleteStreamResponse response(0);

    auto key = namespaceKey(ns, req.stream);

    /// std::scoped_lock guard{mlock};

    auto status = metadb->Delete(rocksdb::WriteOptions(), key);
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

}
