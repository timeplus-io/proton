#pragma once

#include "Loglet.h"

#include <NativeLog/Common/FetchDataInfo.h>
#include <NativeLog/Common/LogAppendInfo.h>
#include <NativeLog/Common/Topic.h>

#include <boost/noncopyable.hpp>

namespace nlog
{
/// A log which presents a surface wrapper of Loglet to simplify Loglet's logic. Log is supposed to more public
/// facing than Loglet and provides higher APIs.
/// Log is multi-thread safe
/// The physical directory structure of a log typically looks like
/// `log_dir` (example: /var/lib/proton/nativelog)
///    | --  offset_checkpoints/
///    | -- topic-shard-0-dir (example: test-0)
///    | -- topic-shard-1-dir (example: test-1)
///    | -- topic-shard-2-dir (example: test-2)
///           |
///           | -- base-offset.log (example: 00000000000000000000.log)
///           | -- base-offset.indexes/
///           | -- shard.metadata
class Log final : private boost::noncopyable
{
public:
    /// @param log_dir The directory from which log segments need to be loaded
    /// @param log_config The configuration settings for the log being loaded
    /// @param topic_id Topic UUID
    /// @param had_clean_shutdown The boolean flag to indicate whether the associated log previously had a clean shutdown
    /// @param log_start_offset The checkpoint of the log start offset
    /// @param recovery_point The checkpoint of the offset at which to begin the recovery
    static std::shared_ptr<Log> create(
        const fs::path & log_dir,
        LogConfigPtr log_config,
        const TopicID & topic_id,
        bool had_clean_shutdown,
        int64_t log_start_offset,
        int64_t recovery_point,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler,
        std::shared_ptr<ThreadPool> adhoc_scheduler);

    /// @param log_start_offset_ The earliest offset allowed to be exposed to NativeLog client
    /// @param loglet_ The corresponding loglet
    /// @param topic_id_ Topic UUID
    Log(int64_t log_start_offset_, LogletPtr loglet_, const TopicID & topic_id_, Poco::Logger * logger_);

    /// Append this message set to the active segment of the Loglet, assigning offsets and shard leader epochs
    /// @param records The records to append
    /// @return Information about the appended messages including the first and the last offset
    LogAppendInfo append(MemoryRecords & records);

    /// Read messages from the log
    /// @param offset The offset to begin reading at. -1 means latest, -2 means earliest
    /// @param max_length The maximum number of bytes to read
    /// @param isolation The fetch isolation, which controls the maximum offset allowed to read
    /// @return The fetch data information including fetch starting offset metadata and message read
    FetchDataInfo read(int64_t offset, int32_t max_length, FetchIsolation isolation = FetchIsolation::FETCH_LOG_END);

    /// Truncate to this log so that it ends with the greatest offset < target_offset
    /// @return True iff target_offset < log_end_offset
    bool trim(int64_t target_offset);

    /// Close the log. The memory mapped buffer for index files and this log will be left open
    /// until the log is deleted
    void close();

    /// Flush all local log segments
    void flush() { flush(loglet->logEndOffset()); }

    /// Flush Loglet segments for all offsets up to `offset - 1`
    /// @param offset The offset to flush up to (non-inclusive); the new recovery point
    void flush(int64_t offset);

    /// Update the high watermark to the new value if and only iff it is larger than the old value.
    /// It is an error to update to a value which is larger than the log end offset
    /// This method is intended to be used by the leader to update the high watermark after follower
    /// fetch offsets have been updated
    /// @return the old high watermark, if updated by the new value
    std::optional<LogOffsetMetadata> maybeIncrementHighWatermark(const LogOffsetMetadata & new_high_watermark);

    int64_t logStartOffset() const { return log_start_offset; }

    /// API calls forwarding to Loglet
    int64_t logEndOffset() const { return loglet->logEndOffset(); }

private:
    LogAppendInfo analyzeAndValidateRecords(MemoryRecords & batch);
    void validateAndAssignOffsets(MemoryRecords & batch, LogAppendInfo & append_info);
    void validateRecord(const Record & record, size_t index, bool compact, LogAppendInfo & append_info);
    void processRecordErrors(const std::vector<RecordError> & record_errors);

    LogSegmentPtr maybeRoll(uint32_t records_size, LogAppendInfo & append_info);
    LogSegmentPtr rollWithoutLock(std::optional<int64_t> expected_next_offset);

    /// Completely delete the log directory and all contents from the file system with no delay
    void remove();

    /// Rename the directory of the Loglet
    void renameDir(const std::string & name);

    /// Delete all data in the log and start at the new offset
    void trimFullyAndStartAt(int64_t new_offset);

    /// @return true if there is new change and need checkpoint it, otherwise return false
    bool needCheckpointRecoveryPoint() const { return loglet->needCheckpointRecoveryPoint(); }

    void beginCheckpointRecoveryPoint(int64_t recovery_point_checkpoint_)
    {
        loglet->beginCheckpointRecoveryPoint(recovery_point_checkpoint_);
    }

    void endCheckpointRecoveryPoint() { loglet->endCheckpointRecoveryPoint(); }

    /// @return true if there is new change and need checkpoint it, otherwise return false
    bool needCheckpointStartOffset() const
    {
        auto base_offset = firstSegment()->baseOffset();
        return log_start_offset > base_offset && log_start_offset != log_start_offset_checkpoint;
    }

    void beginCheckpointStartOffset(int64_t log_start_offset_checkpoint_)
    {
        log_start_offset_checkpoint_prepare = log_start_offset_checkpoint_;
    }
    void endCheckpointStartOffset() { log_start_offset_checkpoint = log_start_offset_checkpoint_prepare; }

    void updateLogStartOffset(int64_t log_start_offset_);

    void maybeIncrementFirstUnstableOffset();
    void maybeIncrementFirstUnstableOffsetWithoutLock();

    void checkLogStartOffset(int64_t offset) const;
    LogOffsetMetadata convertToOffsetMetadataOrThrow(int64_t offset) const;

    int64_t highWatermark() const { return high_watermark_metadata.message_offset; }

    /// Update the high watermark to a new offset. The new high watermark will be
    /// lower bounded by the log start offset and upper bounded by the log end offset.
    /// This is intended to be called when initializing the high watermark or when
    /// updating it on a follower after receiving a Fetch response from the leader
    /// @param hw the suggested new value for the high watermark
    /// @return the updated high watermark offset
    int64_t updateHighWatermark(int64_t hw);

    /// @param with_lock true if the caller holds the lock already, otherwise false
    int64_t updateHighWatermark(const LogOffsetMetadata & high_watermark_metadata_, bool with_lock = false);

    void updateHighWatermarkMetadata(const LogOffsetMetadata & new_high_watermark_metadata);
    void updateHighWatermarkMetadataWithoutLock(const LogOffsetMetadata & new_high_watermark_metadata);
    LogOffsetMetadata fetchHighWatermarkMetadata();
    LogOffsetMetadata fetchHighWatermarkMetadataWithoutLock();

private:
    /// API calls forwarding to Loglet
    const fs::path & parentDir() const { return loglet->parentDir(); }
    const fs::path & namespaceDir() const { return loglet->namespaceDir(); }
    const fs::path & rootDir() const { return loglet->rootDir(); }
    const fs::path & logDir() const { return loglet->logDir(); }
    size_t numberOfSegments() const { return loglet->numberOfSegments(); }
    bool isFuture() const { return loglet->isFuture(); }
    LogSegmentPtr activeSegment() const { return loglet->activeSegment(); }
    LogSegmentPtr firstSegment() const { return loglet->firstSegment(); }
    int64_t lastFlushed() const { return loglet->lastFlushed(); }
    int64_t recoveryPoint() const { return loglet->recoveryPoint(); }
    const LogConfig & config() const { return loglet->config(); }
    const TopicShard & topicShard() const { return loglet->topicShard(); }
    std::shared_ptr<ThreadPool> adhocScheduler() const { return loglet->adhocScheduler(); }

    static TopicShard topicShardFrom(const fs::path & log_dir) { return Loglet::topicShardFrom(log_dir); }
    static std::string logFutureDirName(const TopicShard & topic_shard) { return Loglet::logFutureDirName(topic_shard); }
    static std::string logDirName(const TopicShard & topic_shard) { return Loglet::logDirName(topic_shard); }
    static std::string logDeleteDirName(const TopicShard & topic_shard) { return Loglet::logDeleteDirName(topic_shard); }
    static int64_t offsetFromFileName(const std::string & filename) { return Loglet::offsetFromFileName(filename); }
    static bool isLogFile(const std::string & filename) { return Loglet::isLogFile(filename); }
    static bool isIndexFile(const std::string & filename) { return Loglet::isIndexFile(filename); }
    static fs::path logFile(const fs::path & log_dir, int64_t offset, const std::string & suffix = "")
    {
        return Loglet::logFile(log_dir, offset, suffix);
    }

    static const std::string & METADATA_TOPIC() { return Loglet::METADATA_TOPIC; }
    static const std::string & DELETE_DIR_SUFFIX() { return Loglet::DELETE_DIR_SUFFIX; }
    static const std::string & DELETED_FILE_SUFFIX() { return Loglet::DELETED_FILE_SUFFIX; }
    static const std::string & CLEANED_FILE_SUFFIX() { return Loglet::CLEANED_FILE_SUFFIX; }
    static const std::string & SWAP_FILE_SUFFIX() { return Loglet::SWAP_FILE_SUFFIX; }

    const static int64_t EARLIEST_OFFSET = -2;
    const static int64_t LATEST_OFFSET = -1;

private:
    friend class LogLoader;
    friend class LogManager;

private:
    /// The earliest offset allowed to be exposed to NativeLog client
    /// `start_offset` can be updated by
    /// - user's DeleteRecordRequest
    /// - broker's log retention
    /// - broker's log truncation
    /// - broker's log recovery
    /// The `start_offset` is used to decide the following
    /// - Log deletion. Segment whose nextOffset <= log's `start_offset` can be deleted
    ///   It may trigger log rolling if the active segment is deleted
    /// - Earliest offset of the log in response to ListOffsetRequest. To avoid offsetOutOfRange exception after user seeks
    ///   to the earliest offset, we make sure that `start_offset <= log's high watermark`.
    std::atomic<int64_t> log_start_offset;
    /// Prepare to checkpoint this log start offset
    int64_t log_start_offset_checkpoint_prepare;
    /// Start offset which is already checkpointed. Used to avoid re-checkpointing
    std::atomic<int64_t> log_start_offset_checkpoint;

    /// A mutex that guards all modifications to the following data
    /// - call append / trim / close / renameDir / replaceSegments / deleteSegments / rollSegment / flush to the loglet
    /// - read/update high_watermark_metadata
    /// - read/update first_unstable_offset_metadata
    /// - partition metadata
    /// - update producer_state_manager
    /// - leader epoch cache
    std::mutex lmutex;

    LogletPtr loglet;
    TopicID topic_id;

    std::atomic_flag closed = ATOMIC_FLAG_INIT;

    /// The earliest offset which is part of an incomplete transaction. This is used to compute the
    /// last stable offset (LSO) in ReplicaManager
    std::optional<LogOffsetMetadata> first_unstable_offset_metadata;

    /// Keep track of the current high watermark in order to ensure that segments containing offsets
    /// at or above it are not eligible for deletion. This means that active segment is only eligible
    /// for deletion if the high watermark equals the log end offset (which may never happen for a shard
    /// under consistent load). This is needed to prevent log start offset which is exposed in fetch
    /// responses from getting ahead of the high watermark.
    LogOffsetMetadata high_watermark_metadata;

    Poco::Logger * logger;
};

using LogPtr = std::shared_ptr<Log>;
}
