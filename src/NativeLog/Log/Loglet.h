#pragma once

#include "LogConfig.h"
#include "LogSegments.h"
#include "ProducerStateManager.h"

#include <NativeLog/Base/Utils.h>
#include <NativeLog/Checkpoints/LeaderEpochFileCache.h>
#include <NativeLog/Common/LogDirFailureChannel.h>
#include <NativeLog/Common/StreamShard.h>
#include "NativeLog/Common/LogSequenceMetadata.h"

#include <Common/BackgroundSchedulePool.h>

#include <fmt/format.h>
#include <re2/re2.h>

namespace nlog
{
struct Record;
struct LogAppendDescription;

/// An append-only log which contains a list of segments
/// Loglet is not multi-thread safe
class Loglet final : private boost::noncopyable
{
public:
    /// @param stream_shard_ Stream name / UUID and shard number
    /// @param log_dir_ The directory in which log segments are created
    /// @param log_config_ The log configuration settings
    /// @param recovery_point_ The sn at which to begin the next recovery
    ///        i.e. the first sn that has not been flushed to disk
    /// @param next_sn_meta_ The sn where the next message could be appended
    /// @param segments_ The non-empty log segments recovered from disk
    Loglet(
        const StreamShard & stream_shard_,
        const fs::path & log_dir_,
        LogConfigPtr log_config_,
        int64_t recovery_point_,
        const LogSequenceMetadata & next_sn_meta_,
        LogSegmentsPtr segments_,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
        std::shared_ptr<ThreadPool> adhoc_scheduler_,
        Poco::Logger * logger_);

    /// Read record data up to max_sn_meta indicates
    /// Return record read at sn
    FetchDataDescription fetch(int64_t sn, uint64_t max_size, const LogSequenceMetadata & max_sn_meta, std::optional<uint64_t> position) const;

    /// @param target_sn The sn to truncate to, an upper bound on all sn in the log after
    ///        truncation is complete
    /// @return a list of segments that were scheduled for deletion
    std::vector<LogSegmentPtr> trim(int64_t target_sn);

    void flush(int64_t sn);

    void close();

public:
    static StreamShard streamShardFrom(const fs::path & log_dir_);

private:
    /// Construct a log file name in the given dir with the given base offset and the given suffix
    /// @param dir The directory in which the log will reside
    /// @param offset The base offset of the log file
    /// @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
    static fs::path logFile(const fs::path & dir, int64_t offset, const std::string & suffix = "");

    /// Construct index file folder name in the given dir with the given base offset and the given suffix
    static fs::path indexFileDir(const fs::path & dir, int64_t offset, const std::string & suffix = "");

    /// Return a future directory name for the given stream shard. The name will be in the following format:
    /// `stream_uuid.shard.future` where stream, shard and unique_id are variables
    static std::string logFutureDirName(const StreamShard & stream_shard_) { return logDirNameWithSuffix(stream_shard_, FUTURE_DIR_SUFFIX); }

    /// stream_uuid.shard_id
    static std::string logDirName(const StreamShard & stream_shard_);

    /// Return a directory name to rename the log directory to for async deletion.
    /// The name will be in the following format: "stream_uuid.shard.delete"
    static std::string logDeleteDirName(const StreamShard & stream_shard_) { return logDirNameWithSuffix(stream_shard_, DELETED_FILE_SUFFIX); }

    /// Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
    /// so that ls sorts the files numerically
    static std::string filenamePrefixFromOffset(int64_t offset) { return fmt::format("{:024d}", offset); }

    static std::string logDirNameWithSuffix(const StreamShard & stream_shard_, const std::string & suffix);

    static LeaderEpochFileCachePtr
    createLeaderEpochCache(const fs::path & log_dir, const StreamShard & stream_shard, LogDirFailureChannelPtr log_dir_failure_channel);

    static int64_t sequenceFromFileName(const std::string & filename);

    static bool isIndexFile(const std::string & filename);

    static bool isLogFile(const std::string & filename) { return filename.ends_with(LOG_FILE_SUFFIX); }

    static void rebuildProducerState(
        ProducerStateManagerPtr producer_state_manager_,
        LogSegmentsPtr segments_,
        int64_t start_offset_,
        int64_t last_offset_,
        bool reload_from_clean_shutdown_);

private:
    void append(const ByteVector & record, const LogAppendDescription & append_info);

    /// Translate timestamp to sn
    /// @param ts Timestamp
    /// @param append_time Is the ts append time or event time
    /// @return The very start sn for the timestamp ts
    int64_t sequenceForTimestamp(int64_t ts, bool append_time) const;

    LogSegmentPtr roll(std::optional<int64_t> expected_next_offset);

    /// Completely delete all segments with no delay
    std::vector<LogSegmentPtr> removeAllSegments();

    /// This method deletes the given log segments by doing the following for each of them
    /// - It removes the segment from the segment map so that it will no longer be used for reads
    /// - It renames the index and log files by appending .deleted to the respective file name
    /// - It can either schedule an async delete operation to occur in the future or perform the deletion synchronously.
    /// Async deletion allows reads to happen concurrently without synchronization and without the possibility of
    /// physically deleting a file while it is being read
    void removeSegments(const std::vector<LogSegmentPtr> & segments_to_delete, bool async);

    /// Perform physical deletion of the index and the log files for the given segment.
    /// Prior to the deletion, the index and log files are renamed by appending .deleted to the
    /// respective file name. Allows these files to be optionally deleted asynchronously
    /// This method assumes the file exists.
    void removeAllSegmentFiles(const std::vector<LogSegmentPtr> & segments_to_delete, bool async);

    /// Completely delete this log directory with no delay
    void removeEmptyDir();

    /// Rename the directory of the log
    bool renameDir(const std::string & name);

    void markFlushed(int64_t offset);

    LogSequenceMetadata convertToSequenceMetadataOrThrow(int64_t sn) const;

    /// The offset of the next message that will be appended to the log
    int64_t logEndSequence() const;

    int64_t unflushedRecords() const { return logEndSequence() - recoveryPoint(); }

    LogSequenceMetadata logEndSequenceMetadata() const;

    void updateLogEndSequence(int64_t end_sn, int64_t segment_base_sn, uint64_t segment_pos);

    void updateRecoveryPoint(int64_t new_recovery_point) { recovery_point = new_recovery_point; }

    bool needCheckpointRecoveryPoint() const { return recovery_point != recovery_point_checkpoint; }

    void beginCheckpointRecoveryPoint(int64_t recovery_point_checkpoint_)
    {
        recovery_point_checkpoint_prepare = recovery_point_checkpoint_;
    }

    void endCheckpointRecoveryPoint() { recovery_point_checkpoint = recovery_point_checkpoint_prepare; }

    LogSegmentPtr firstSegment() const { return segments->firstSegment(); }
    LogSegmentPtr activeSegment() const { return segments->activeSegment(); }

    int64_t lastFlushed() const { return last_flushed_ms; }

    int64_t recoveryPoint() const { return recovery_point; }

    const StreamShard & streamShard() const { return stream_shard; }

    const LogConfig & config() const { return *log_config; }

    const fs::path & parentDir() const { return parent_dir; }

    const fs::path & namespaceDir() const { return parent_dir; }

    const fs::path & rootDir() const { return root_dir; }

    const fs::path & logDir() const { return log_dir; }

    bool isFuture() const { return log_dir.filename().string().ends_with(FUTURE_DIR_SUFFIX); }

    size_t numberOfSegments() const { return segments->size(); }

    std::shared_ptr<ThreadPool> adhocScheduler() const { return adhoc_scheduler; }

    friend class Log;
    friend class LogSegment;

private:
    /// A log file
    inline static const std::string LOG_FILE_SUFFIX = ".log";

    /// An index file
    inline static const std::string INDEX_FILE_SUFFIX = ".indexes";

    /// A file that is scheduled to be deleted
    inline static const std::string DELETED_FILE_SUFFIX = ".deleted";

    /// A temporary file that is being used for log cleaning
    inline static const std::string CLEANED_FILE_SUFFIX = ".cleaned";

    /// A temporary file used when swapping files into the log
    inline static const std::string SWAP_FILE_SUFFIX = ".swap";

    /// A directory that is scheduled to be deleted
    inline static const std::string DELETE_DIR_SUFFIX = ".delete";

    /// A directory that is used for future partition
    inline static const std::string FUTURE_DIR_SUFFIX = ".future";

    inline static auto DELETE_DIR_PATTERN = re2::RE2{"^(\\S+)\\.(\\d+)\\.(\\S+)\\.delete$"};
    inline static auto FUTURE_DIR_PATTERN = re2::RE2{"^(\\S+)\\.(\\d+)\\.(\\S+)\\.future$"};
    inline static auto LOG_DIR_PATTERN = re2::RE2{"^(\\S+)\\.(\\d+)$"};

    /// static constexpr int64_t UNKNOWN_SN = -1;

    /// inline static std::string METADATA_STREAM = "__cluster_metadata";

private:
    std::filesystem::path log_dir;
    std::filesystem::path parent_dir;
    std::filesystem::path root_dir;

    StreamShard stream_shard;
    LogConfigPtr log_config;

    /// Recovery point offset are periodically checkpointed in persistent store.
    /// It is used when system reboots and the log is loaded from disk. It is
    /// an lower bound offset which we are sure that all messages which have
    /// offset less than or equal to recovery point offset are durable in
    /// file system.
    /// There are several scenarios we will need update recovery point
    /// 1. Whenever we do log flush (during append for example and when a flush deadline has reached)
    /// 2. Periodically flush. A background thread will check if there are any dirty logs needed to be flushed
    /// 3. During LogManager shutdown. To make sure all messages in OS page cache are flushed
    std::atomic<int64_t> recovery_point;
    /// Prepare to checkpoint this recovery point offset
    int64_t recovery_point_checkpoint_prepare;
    /// Last checkpointed recovery point. Used to avoid re-checkpointing
    std::atomic<int64_t> recovery_point_checkpoint;

    mutable std::mutex next_sn_mutex;
    LogSequenceMetadata next_sn_meta;

    LogSegmentsPtr segments;

    int64_t last_flushed_ms;

    LogDirFailureChannelPtr log_dir_failure_channel;

    /// Scheduler for scheduling repeated tasks
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;

    /// Scheduler for scheduling ad-hoc tasks which usually run once
    std::shared_ptr<ThreadPool> adhoc_scheduler;

    Poco::Logger * logger;
};

using LogletPtr = std::shared_ptr<Loglet>;
}
