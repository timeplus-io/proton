#include "Checkpoints.h"

#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

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
    std::vector<char> writeKey(const std::string & ns, const StreamShard & stream_shard)
    {
        std::vector<char> key;

        /// '/' + namespace + '/' + stream_id + '/' + shard
        key.resize(1 + ns.size() + 1 + sizeof(stream_shard.stream.id) + 1 + sizeof(int32_t));

        DB::WriteBufferFromVector<std::vector<char>> wbuf(key);

        /// Write separator
        wbuf.write('/');

        /// Write namespace
        wbuf.write(ns.data(), ns.size());

        /// Write separator
        wbuf.write('/');

        /// Write stream
        DB::writeBinary(stream_shard.stream.id, wbuf);

        /// Write separator
        wbuf.write('/');

        /// Write shard binary
        DB::writeIntBinary(stream_shard.shard, wbuf);

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
    /// default cf : log recovery point sn
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    /// log_start cf: log start sn
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("log_start", rocksdb::ColumnFamilyOptions()));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;

    rocksdb::DB * db;
    auto status = rocksdb::DB::Open(options, ckpt_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open index db, {}", status.ToString());

    checkpoints.reset(db);
    recovery_sn_cf_handle = cf_handles[0];
    start_sn_cf_handle = cf_handles[1];
}

Checkpoints::~Checkpoints()
{
    for (auto * cf_handle : {recovery_sn_cf_handle, start_sn_cf_handle})
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

void Checkpoints::updateLogRecoveryPointSequences(const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns)
{
    updateSequences(sns, recovery_sn_cf_handle);
}

void Checkpoints::updateLogStartSequences(const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns)
{
    updateSequences(sns, start_sn_cf_handle);
}

void Checkpoints::updateSequences(
    const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns, rocksdb::ColumnFamilyHandle * cf_handle)
{
    rocksdb::WriteBatch batch;

    /// We have quite a few memory allocation in this loop
    for (const auto & ns_sns : sns)
    {
        const auto & ns = ns_sns.first;
        for (const auto & shard_sn : ns_sns.second)
        {
            auto key{writeKey(ns, shard_sn.stream_shard)};

            /// batch.Put will copy the key/value slices over
            batch.Put(
                cf_handle,
                rocksdb::Slice{key.data(), key.size()},
                rocksdb::Slice{reinterpret_cast<const char *>(&shard_sn.sn), sizeof(shard_sn.sn)});
        }
    }

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to update log recovery checkpoints, error={}", status.ToString());
        throw DB::Exception(
            DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Failed to update log recovery checkpoints for error={}", status.ToString());
    }

    sync();
}

std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> Checkpoints::readLogRecoveryPointSequences()
{
    return readSequences(recovery_sn_cf_handle);
}

std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> Checkpoints::readLogStarSequences()
{
    return readSequences(start_sn_cf_handle);
}

std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>>
Checkpoints::readSequences(rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> results;
    std::unique_ptr<rocksdb::Iterator> iter{checkpoints->NewIterator(rocksdb::ReadOptions{}, cf_handle)};

    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        /// Parse key /namespace/stream_id/shard
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

        assert(static_cast<size_t>(end_pos - start_pos) == sizeof(StreamID) + 1 + sizeof(int32_t));

        /// Stream ID
        StreamID stream_id = DB::UUIDHelpers::Nil;
        ::memcpy(&stream_id, start_pos, sizeof(stream_id));

        start_pos += sizeof(stream_id) + 1;

        assert(*(start_pos - 1) == '/');

        /// Shard
        assert(end_pos - start_pos == sizeof(int32_t));

        /// This is internal implementation of DB::readIntBinary
        int32_t shard = 0;
        ::memcpy(&shard, start_pos, sizeof(int32_t));

        results[ns].emplace(StreamShard{"", stream_id, shard}, unalignedLoad<int64_t>(value.data()));
    }
    return results;
}

void Checkpoints::removeLogSequences(const std::string & ns, const Stream & stream)
{
    assert(stream.id != DB::UUIDHelpers::Nil);

    auto key_start{writeKey(ns, StreamShard{stream, 0})};
    /// RocksDB by default is comparing bytes, 0xFFFF,FFFFF,FFFF,FFFF has the biggest bytes in int32_t
    auto key_end{writeKey(ns, StreamShard{stream, std::numeric_limits<int32_t>::min()})};
    rocksdb::Slice key_start_s{key_start.data(), key_start.size()};
    rocksdb::Slice key_end_s{key_end.data(), key_end.size()};

    /// Range delete
    rocksdb::WriteBatch batch;
    batch.DeleteRange(recovery_sn_cf_handle, key_start_s, key_end_s);
    batch.DeleteRange(start_sn_cf_handle, key_start_s, key_end_s);

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        LOG_ERROR(
            logger,
            "Failed to delete recovery_sn and log_start sns for stream={} in namespace={}, error={}",
            stream.name,
            ns,
            status.ToString());

        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
            "Deleted recovery_sn and log_start sns for stream={} in namespace={}, error={}",
            stream.name,
            ns,
            status.ToString());
    }

    sync();

    LOG_INFO(logger, "Deleted recovery_sn and log_start sns for stream={} in namespace={}", stream.name, ns);
}

void Checkpoints::removeLogSequences(const std::string & ns, const StreamShard & stream_shard)
{
    const auto & stream_shard_str = stream_shard.string();

    rocksdb::WriteBatch batch;

    auto key{writeKey(ns, stream_shard)};
    batch.Delete(recovery_sn_cf_handle, rocksdb::Slice{key.data(), key.size()});
    batch.Delete(start_sn_cf_handle, rocksdb::Slice{key.data(), key.size()});

    auto status = checkpoints->Write(rocksdb::WriteOptions{}, &batch);
    if (!status.ok())
    {
        const auto & status_str = status.ToString();

        LOG_ERROR(logger, "Deleted recovery_sn and log_start sns for shard={} in namespace={} error={}", stream_shard_str, ns, status_str);

        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
            "Deleted recovery_sn and log_start sns for shard={} in namespace={} error={}",
            stream_shard_str,
            ns,
            status_str);
    }

    LOG_INFO(logger, "Deleted recovery_sn and log_start sns for shard={} namespace={}", stream_shard_str, ns);
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
