#pragma once

#include <Storages/Streaming/StreamShard.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>

#include <pcg_random.hpp>

namespace DB
{

/// StreamKVShard contains 2 parts
/// 1. one shard for streaming log store
/// 2. one shard for historical store (optional).
/// A background process consumes data from log store shard and then commits the data
/// to the shard of historical store. Please note that the historical store shard is optional.
class StreamKVShard: public StreamShard
{
public:
    StreamKVShard(
        Int32 replication_factor_,
        Int32 shards_,
        Int32 shard_,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach_,
        ContextMutablePtr context_,
        const String & date_column_name_,
        const MergeTreeData::MergingParams & merging_params_,
        std::unique_ptr<StreamSettings> settings_,
        bool has_force_restore_data_flag_,
        MergeTreeData * storage_stream_,
        Poco::Logger * log_);

    ~StreamKVShard() override;

private:
    using SequencePair = std::pair<nlog::RecordSN, nlog::RecordSN>;

    void doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges) override;

    friend struct StreamCallbackData;
    friend class StorageKV;

private:
    std::shared_ptr<StorageEmbeddedRocksDB> storage_rocksdb;

};

}
