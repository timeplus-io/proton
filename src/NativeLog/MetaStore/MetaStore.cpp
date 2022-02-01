#include "MetaStore.h"
#include "NamespaceKey.h"

#include <NativeLog/Schemas/MemoryTopicMetadata.h>

#include <Common/Exception.h>
#include <base/logger_useful.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>

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

TopicInfo getTopicInfoFrom(const MemoryTopicMetadata & topic)
{
    TopicInfo topic_info;
    topic_info.version = topic.version();
    topic_info.create_timestamp = topic.createTimestamp();
    topic_info.last_modify_timestamp = topic.lastModifyTimestamp();
    topic_info.ns = topic.ns().str();
    topic_info.name = topic.name().str();
    topic_info.id = topic.id();
    topic_info.partitions = topic.partitions();
    topic_info.replicas = topic.replicas();
    topic_info.compacted = topic.compacted();
    return topic_info;
}
}

MetaStore::MetaStore(const MetaStoreConfig & config_)
    : config(config_), logger(&Poco::Logger::get("metastore"))
{
    if (!fs::exists(config.meta_dir))
        fs::create_directories(config.meta_dir);

    /// Topics metadata resides in default column family. If necessary create other column family for other metadata
    rocksdb::DB * db = nullptr;
    auto status = rocksdb::DB::Open(getRocksDBOptions(), config.meta_dir, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open metadb {}", status.ToString());

    metadb.reset(db);
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

CreateTopicResponse MetaStore::createTopic(const std::string & ns, const CreateTopicRequest & req)
{
    assert(!ns.empty());
    assert(!req.name.empty());

    auto key = namespaceKey(ns, req.name);
    auto topic{MemoryTopicMetadata::buildFrom(ns, req)};
    auto value = topic.data();
    std::string rvalue;

    std::lock_guard guard{mlock};
    auto status = metadb->Get(rocksdb::ReadOptions{}, key, &rvalue);
    if (!status.IsNotFound())
        /// topic already exists
        throw DB::Exception(DB::ErrorCodes::RESOURCE_ALREADY_EXISTS, "topic {} in namespace {} already exists", req.name, ns);

    status = metadb->Put(rocksdb::WriteOptions{}, key, rocksdb::Slice(value.data(), value.size()));
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to create topic={}", key);
        throw DB::Exception(DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Failed to create topic={}", req.name);
    }

    CreateTopicResponse response(CreateTopicResponse::from(req));
    response.topic_info.version = topic.version();
    response.topic_info.id = topic.id();
    response.topic_info.ns = ns;
    response.topic_info.create_timestamp = topic.createTimestamp();
    response.topic_info.last_modify_timestamp = topic.lastModifyTimestamp();

    return response;
}

DeleteTopicResponse MetaStore::deleteTopic(const std::string & ns, const DeleteTopicRequest & req)
{
    assert(!ns.empty());
    assert(!req.name.empty());

    auto key = namespaceKey(ns, req.name);
    auto status = metadb->Delete(rocksdb::WriteOptions(), key);
    if (!status.ok() && !status.IsNotFound())
    {
        LOG_ERROR(logger, "Failed to delete topic={}", key);
        throw DB::Exception(DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Failed to delete topic={} in namespace={}", req.name, ns);
    }

    DeleteTopicResponse response;
    response.error.error_message = status.ToString();
    return response;
}

ListTopicsResponse MetaStore::listTopics(const std::string & ns, const ListTopicsRequest & req) const
{
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> iter;
    if (ns.empty())
    {

        iter.reset(metadb->NewIterator(rocksdb::ReadOptions{}));
        iter->SeekToFirst();
    }
    else
    {
        /// options.total_order_seek = true;
        options.auto_prefix_mode = true;
        options.prefix_same_as_start = true;

        iter.reset(metadb->NewIterator(options));
        auto key = namespaceKey(ns, req.topic);
        iter->Seek(key);
    }

    ListTopicsResponse response;
    while(iter->Valid())
    {
        /// assert(iter->key().starts_with(key));

        const auto & value = iter->value();
        MemoryTopicMetadata topic{std::span<char>(const_cast<char*>(value.data()), value.size())};

        response.topics.push_back(getTopicInfoFrom(topic));
        iter->Next();
    }

    return response;
}
}
