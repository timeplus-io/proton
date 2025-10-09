#pragma once

#include <Cluster/Common/EpochSequence.h>

#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace rocksdb
{
class DB;
class ColumnFamilyHandle;
}

namespace cluster::nlog
{
/// LeaderEpochCheckpoint persists the mapping of (LeaderEpoch => Starting sequence number of the leader epoch)
/// It is not thread-safe
class LeaderEpochIndexRocks
{
public:
    explicit LeaderEpochIndexRocks(const fs::path & ckpt_dir, LoggerPtr logger_);
    ~LeaderEpochIndexRocks();

    /// return true if updated / inserted otherwise false
    bool maybeIndexEpochStartSequence(uint64_t epoch, int64_t sn);
    std::optional<EpochSequence> latestLeaderEpochSequence() const;
    std::optional<EpochSequence> previousLeaderEpochSequence() const;
    std::optional<uint64_t> latestLeaderEpoch() const;
    std::optional<uint64_t> previousLeaderEpoch() const;

    std::optional<EpochSequence> leaderEpochSequenceFor(int64_t sn) const;

    /// Delete all indexed entries
    void clear();

    /// Truncate all indexed entries which has sequence in [sn, ...)
    void trim(int64_t sn);

private:
    void loadLatestEpoch();
    void updateCached(uint64_t epoch, int64_t sn);

private:
    std::unique_ptr<rocksdb::DB> epoch_db;
    rocksdb::ColumnFamilyHandle * epoch_sn_cf_handle = nullptr;
    rocksdb::ColumnFamilyHandle * sn_epoch_cf_handle = nullptr;

    std::optional<EpochSequence> latest_epoch_sn_cached;
    std::optional<EpochSequence> previous_epoch_sn_cached;

    LoggerPtr logger;
};

using LeaderEpochCheckpointRocksPtr = std::unique_ptr<LeaderEpochIndexRocks>;
}
