#include "Checkpoints.h"

#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int CANNOT_OPEN_DATABASE;
    const extern int INSERT_KEY_VALUE_FAILED;
    const extern int DELETE_KEY_VALUE_FAILED;
}
}

namespace nlog
{
namespace
{
    std::vector<char> writeKey(const std::string & ns, const TopicShard & topic_shard)
    {
        std::vector<char> key;

        /// '/' + namespace + '/' + topic_name + '/' + shard
        key.resize(ns.size() + 1 + topic_shard.topic.size() + 1 + sizeof(uint32_t));

        DB::WriteBufferFromVector<std::vector<char>> wbuf(key);

        /// Write separator
        wbuf.write('/');

        /// Write namespace
        wbuf.write(ns.data(), ns.size());

        /// Write separator
        wbuf.write('/');

        /// Write topic
        wbuf.write(topic_shard.topic.data(), topic_shard.topic.size());

        /// Write separator
        wbuf.write('/');

        /// Write shard binary
        DB::writeIntBinary(topic_shard.shard, wbuf);

        /// We don't need call wbuf.finalize since we know exactly the layout
        // of our buffer, so there will be no garbage at the end

        return key;
    }
}

Checkpoints::Checkpoints(const fs::path & ckpt_dir, Poco::Logger * logger_) : logger(logger_)
{
    rocksdb::Options options;
    options.num_levels = 3;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    /// default cf : log recovery point offset
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    /// log_start cf: log start offset
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("log_start", rocksdb::ColumnFamilyOptions()));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;

    rocksdb::DB * db;
    auto status = rocksdb::DB::Open(options, ckpt_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open index db, {}", status.ToString());

    checkpoints.reset(db);
    recovery_offset_cf_handle = cf_handles[0];
    start_offset_cf_handle = cf_handles[1];
}

Checkpoints::~Checkpoints()
{
    for (auto * cf_handle : {recovery_offset_cf_handle, start_offset_cf_handle})
    {
        if (cf_handle != nullptr)
        {
            auto status = checkpoints->DestroyColumnFamilyHandle(cf_handle);
            if (!status.ok())
                LOG_ERROR(logger, "Failed to destroy column family handle");
        }
    }

    auto status = checkpoints->Close();
    if (!status.ok())
        LOG_ERROR(logger, "Failed to close indexes db");
}

void Checkpoints::updateLogRecoveryPointOffsets(const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets)
{
    updateOffsets(offsets, recovery_offset_cf_handle);
}

void Checkpoints::updateLogStartOffsets(const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets)
{
    updateOffsets(offsets, start_offset_cf_handle);
}

void Checkpoints::updateOffsets(
    const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets, rocksdb::ColumnFamilyHandle * cf_handle)
{
    rocksdb::WriteBatch batch;

    /// We have quite a few memory allocation in this loop
    for (const auto & ns_offsets : offsets)
    {
        const auto & ns = ns_offsets.first;
        for (const auto & shard_offset : ns_offsets.second)
        {
            auto key{writeKey(ns, shard_offset.topic_shard)};

            /// batch.Put will copy the key/value slices over
            batch.Put(
                cf_handle,
                rocksdb::Slice{key.data(), key.size()},
                rocksdb::Slice{reinterpret_cast<const char *>(&shard_offset.offset), sizeof(shard_offset.offset)});
        }
    }

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to update log recovery checkpoints, error={}", status.ToString());
        throw DB::Exception(DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Failed to update log recovery checkpoints for error={}", status.ToString());
    }

    sync();
}

std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> Checkpoints::readLogRecoveryPointOffsets()
{
    return readOffsets(recovery_offset_cf_handle);
}

std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> Checkpoints::readLogStartOffsets()
{
    return readOffsets(start_offset_cf_handle);
}

std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>>
Checkpoints::readOffsets(rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> results;
    std::unique_ptr<rocksdb::Iterator> iter{checkpoints->NewIterator(rocksdb::ReadOptions{}, cf_handle)};

    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        /// Parse key /namespace/topic/shard
        auto key{iter->key()};
        auto value{iter->value()};
        auto start_pos = key.data() + 1;
        auto end_pos = key.data() + key.size();

        /// We don't use DB::ReadBuffer here since we know exactly the layout of the key
        assert(key.data()[0] == '/');
        assert(key.size() > 2);
        assert(value.size() == sizeof(int64_t));

        /// Namespace
        auto pos = std::find(start_pos, end_pos, '/');
        assert(pos != end_pos);
        std::string ns(start_pos, pos - start_pos);
        start_pos = pos + 1;

        /// Topic name
        pos = std::find(start_pos, end_pos, '/');
        assert(pos != end_pos);
        std::string topic(start_pos, pos - start_pos);
        start_pos = pos + 1;

        /// Shard
        assert(end_pos - start_pos == sizeof(uint32_t));

        /// This is internal implementation of DB::readIntBinary
        uint32_t shard = 0;
        ::memcpy(&shard, start_pos, sizeof(uint32_t));

        results[ns].emplace(TopicShard{std::move(topic), shard}, *reinterpret_cast<const int64_t *>(value.data()));
    }
    return results;
}

void Checkpoints::removeLogOffsets(const std::string & ns, const std::string & topic)
{
    assert(!topic.empty());

    auto key_start{writeKey(ns, TopicShard{topic, 0})};
    auto key_end{writeKey(ns, TopicShard{topic, std::numeric_limits<uint32_t>::max()})};
    rocksdb::Slice key_start_s{key_start.data(), key_start.size()};
    rocksdb::Slice key_end_s{key_end.data(), key_end.size()};

    /// Range delete
    rocksdb::WriteBatch batch;
    batch.DeleteRange(recovery_offset_cf_handle, key_start_s, key_end_s);
    batch.DeleteRange(start_offset_cf_handle, key_start_s, key_end_s);

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        LOG_ERROR(
            logger,
            "Failed to delete recovery_offset and log_start offsets for topic={} in namespace={}, error={}",
            topic,
            ns,
            status.ToString());

        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
            "Deleted recovery_offset and log_start offsets for topic={} in namespace={}, error={}",
            topic,
            ns,
            status.ToString());
    }

    sync();

    LOG_INFO(
        logger,
        "Deleted recovery_offset and log_start offsets for topic={} in namespace={}",
        topic,
        ns);
}

void Checkpoints::removeLogOffsets(const std::string & ns, const TopicShard & topic_shard)
{
    const auto & topic_shard_str = topic_shard.string();

    rocksdb::WriteBatch batch;

    auto key{writeKey(ns, topic_shard)};
    batch.Delete(recovery_offset_cf_handle, rocksdb::Slice{key.data(), key.size()});
    batch.Delete(start_offset_cf_handle, rocksdb::Slice{key.data(), key.size()});

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        const auto & status_str = status.ToString();

        LOG_ERROR(
            logger,
            "Deleted recovery_offset and log_start offsets for shard={} in namespace={} error={}",
            topic_shard_str,
            ns,
            status_str);

        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
            "Deleted recovery_offset and log_start offsets for shard={} in namespace={} error={}",
            topic_shard_str,
            ns,
            status_str);
    }

    LOG_INFO(
        logger,
        "Deleted recovery_offset and log_start offsets for shard={} namespace={}",
        topic_shard_str, ns);
}

bool Checkpoints::sync()
{
    /// If sync failed, we don't throw exception, otherwise let end user to check the return value
    auto status = checkpoints->FlushWAL(true);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to flush checkpoints, error={}", status.ToString());
        return false;
    }
    return true;
}
}
