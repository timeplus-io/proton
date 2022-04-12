#pragma once

#include "StreamShardSequence.h"

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
/// - Log recovery point sn checkpoints for each stream shard in every namespace, which has the following entry
///   (ns, stream_shard) -> recovery sn
/// - Log start sequence checkpoints for each stream shard in every namespace, which has the following entry
///   (ns, stream_shard) -> start sn
/// Recovery point sn is the last stable sn of a shard
/// Log start sn is the base sn of the active segment in a shard
/// During the startup of NativeLog, LogManager needs get these sns for loading all segments for all shards
/// These checkpoints are per root log directory
class Checkpoints final : private boost::noncopyable
{
public:
    /// @ckpt_dir : Checkpoint directory
    Checkpoints(const fs::path & ckpt_dir, Poco::Logger * logger_);
    ~Checkpoints();

    /// Update latest log recovery point sns to checkpoint
    void updateLogRecoveryPointSequences(const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns);

    /// Update latest log start sns to checkpoint
    void updateLogStartSequences(const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns);

    /// Read latest log recovery point sns from checkpoint
    std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> readLogRecoveryPointSequences();

    /// Read latest log start sns from checkpoint
    std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> readLogStarSequences();

    /// Delete log recovery point sns and log start sns from checkpoint in a single atomic transaction
    /// when a stream is removed
    void removeLogSequences(const std::string & ns, const Stream & stream);

    /// Delete log recovery point sn and log start sn from checkpoint in single atomic transaction
    /// when a shard is removed
    void removeLogSequences(const std::string & ns, const StreamShard & stream_shard);

    /// Flush the in memory checkpoints to persistent store to make sure it is crash consistent
    /// @return true if success otherwise return false
    bool sync();

private:
    void
    updateSequences(const std::unordered_map<std::string, std::vector<StreamShardSequence>> & sns, rocksdb::ColumnFamilyHandle * cf_handle);
    std::unordered_map<std::string, std::unordered_map<StreamShard, int64_t>> readSequences(rocksdb::ColumnFamilyHandle * cf_handle) const;
    /// void removeSNs(const std::string & ns, const std::string & stream, rocksdb::ColumnFamilyHandle * cf_handle);

private:
    std::unique_ptr<rocksdb::DB> checkpoints;
    rocksdb::ColumnFamilyHandle * recovery_sn_cf_handle;
    rocksdb::ColumnFamilyHandle * start_sn_cf_handle;

    Poco::Logger * logger;
};

using CheckpointsPtr = std::shared_ptr<Checkpoints>;
}
