#pragma once

#include "TopicShardOffset.h"

#include <NativeLog/Base/Stds.h>

#include <boost/noncopyable.hpp>
#include <rocksdb/db.h>

namespace Poco
{
class Logger;
}

namespace nlog
{
/// Checkpoints contain
/// - Log recovery point offset checkpoints for each topic shard in every namespace, which has the following entry
///   (ns, topic_shard) -> recovery offset
/// - Log start offset checkpoints for each topic shard in every namespace, which has the following entry
///   (ns, topic_shard) -> start offset
/// Recovery point offset is the last stable offset of a shard
/// Log start offset is the base offset of the active segment in a shard
/// During the startup of NativeLog, LogManager needs get these offsets for loading all segments for all shards
/// These checkpoints are per root log directory
class Checkpoints final : private boost::noncopyable
{
public:
    /// @ckpt_dir : Checkpoint directory
    Checkpoints(const fs::path & ckpt_dir, Poco::Logger * logger_);
    ~Checkpoints();

    /// Update latest log recovery point offsets to checkpoint
    void updateLogRecoveryPointOffsets(const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets);

    /// Update latest log start offsets to checkpoint
    void updateLogStartOffsets(const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets);

    /// Read latest log recovery point offsets from checkpoint
    std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> readLogRecoveryPointOffsets();

    /// Read latest log start offsets from checkpoint
    std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> readLogStartOffsets();

    /// Delete log recovery point offsets and log start offsets from checkpoint in a single atomic transaction
    /// when a topic is removed
    void removeLogOffsets(const std::string & ns, const std::string & topic);

    /// Delete log recovery point offset and log start offset from checkpoint in single atomic transaction
    /// when a shard is removed
    void removeLogOffsets(const std::string & ns, const TopicShard & topic_shard);

    /// Flush the in memory checkpoints to persistent store to make sure it is crash consistent
    /// @return true if success otherwise return false
    bool sync();

private:
    void
    updateOffsets(const std::unordered_map<std::string, std::vector<TopicShardOffset>> & offsets, rocksdb::ColumnFamilyHandle * cf_handle);
    std::unordered_map<std::string, std::unordered_map<TopicShard, int64_t>> readOffsets(rocksdb::ColumnFamilyHandle * cf_handle) const;
    /// void removeOffsets(const std::string & ns, const std::string & topic, rocksdb::ColumnFamilyHandle * cf_handle);

private:
    std::unique_ptr<rocksdb::DB> checkpoints;
    rocksdb::ColumnFamilyHandle * recovery_offset_cf_handle;
    rocksdb::ColumnFamilyHandle * start_offset_cf_handle;

    Poco::Logger * logger;
};

using CheckpointsPtr = std::shared_ptr<Checkpoints>;
}
