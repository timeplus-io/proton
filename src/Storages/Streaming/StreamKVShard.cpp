#include <Storages/Streaming/StreamKVShard.h>
//#include "StreamCallbackData.h"
//#include "StreamingBlockReaderKafka.h"
//#include "storageUtil.h"

//#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
//#include <KafkaLog/KafkaWALCommon.h>
//#include <NativeLog/Server/NativeLog.h>
//#include <Storages/MergeTree/MergeTreeSink.h>
//#include <Storages/StorageMergeTree.h>
#include <Common/ProtonCommon.h>
//#include <Common/setThreadName.h>
#include <Common/timeScale.h>

#include <Storages/Streaming/StreamShard.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INVALID_CONFIG_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
}

namespace {
inline void assignIndexTime1(ColumnWithTypeAndName * index_time_col)
{
    if (!index_time_col)
        return;

    /// index_time_col->column
    ///    = index_time_col->type->createColumnConst(moved_block.rows(), nowSubsecond(3))->convertToFullColumnIfConst();

    const auto * type = typeid_cast<const DataTypeDateTime64 *>(index_time_col->type.get());
    if (type)
    {
        auto scale = type->getScale();
        auto * col = typeid_cast<ColumnDecimal<DateTime64> *>(index_time_col->column->assumeMutable().get());
        assert(col);

        auto now = nowSubsecond(scale).get<DateTime64>();
        auto & data = col->getData();
        for (auto & item : data)
            item = now;
    }
}
}

StreamKVShard::StreamKVShard(
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
    Poco::Logger * log_)
    : StreamShard(
        replication_factor_, shards_, shard_, table_id_, relative_data_path_,
        metadata_, attach_, context_, date_column_name_, merging_params_, std::move(settings_),
        has_force_restore_data_flag_, storage_stream_, log_)
{
    if (!relative_data_path_.empty() && !isInmemory())
    {
        auto shard_path = fmt::format("{}{}/", relative_data_path_, shard_);
        storage_rocksdb = std::make_shared<StorageEmbeddedRocksDB>(
            table_id_,
            shard_path,
            metadata_,
            attach_,
            context_,
            date_column_name_);

        LOG_INFO(log, "Load rocksdb from {} of shard {}", shard_path, shard);
    }
}

StreamKVShard::~StreamKVShard()
{
    shutdown();
}

void StreamKVShard::doCommit(Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges)
{
    {
        std::lock_guard lock(sns_mutex);
        assert(seq_pair.first > last_sn);
        /// We are sequentially consuming records, so seq_pair is always increasing
        outstanding_sns.push_back(seq_pair);

        /// After deduplication, we may end up with empty block
        /// We still mark these deduped blocks committed and moving forward
        /// the offset checkpointing
        if (!block)
        {
            progressSequencesWithoutLock(seq_pair);
            return;
        }

        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// Commit blocks to file system async
    part_commit_pool.scheduleOrThrowOnError([&,
                                             moved_block = std::move(block),
                                             moved_seq = std::move(seq_pair),
                                             moved_keys = std::move(keys),
                                             moved_sequence_ranges = std::move(missing_sequence_ranges),
                                             this]() mutable {
        while (!stopped.test())
        {
            try
            {
                auto sink = storage_rocksdb->write(nullptr, storage_rocksdb->getInMemoryMetadataPtr(), storage_stream->getContext());


                auto * rocksdb_sink = static_cast<EmbeddedRocksDBSink *>(sink.get());

                /// Reset index time here
                assignIndexTime1(const_cast<ColumnWithTypeAndName *>(moved_block.findByName(ProtonConsts::RESERVED_INDEX_TIME)));

                rocksdb_sink->consume(Chunk(moved_block.getColumns(), moved_block.rows()));
                break;
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "Failed to commit rows={} for shard={} exception={} to file system",
                    moved_block.rows(),
                    shard,
                    getCurrentExceptionMessage(true, true));
                /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }

        progressSequences(moved_seq);
    });

    commitSN();
}

}
