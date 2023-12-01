#pragma once

#include "Loglet.h"

#include <NativeLog/Cache/TailCache.h>
#include <NativeLog/Common/FetchDataDescription.h>
#include <NativeLog/Common/LogAppendDescription.h>

#include <boost/noncopyable.hpp>

namespace nlog
{
struct Record;
struct StreamShard;

/// A log which presents a surface wrapper of Loglet to simplify Loglet's logic. Log is supposed to more public
/// facing than Loglet and provides higher APIs.
/// Log is multi-thread safe
/// The physical directory structure of a log typically looks like
/// `log_dir` (example: /var/lib/proton/nativelog)
///    | -- sn_ckpts/
///    | -- stream-shard-0-dir (example: test-0)
///    | -- stream-shard-1-dir (example: test-1)
///    | -- stream-shard-2-dir (example: test-2)
///           |
///           | -- base-sn.log (example: 00000000000000000000.log)
///           | -- base-sn.indexes/
///           | -- shard.metadata
class Log final : private boost::noncopyable
{
public:
    /// @param stream_shard Stream name / UUID and shard number
    /// @param log_dir The directory from which log segments need to be loaded
    /// @param log_config The configuration settings for the log being loaded
    /// @param had_clean_shutdown The boolean flag to indicate whether the associated log previously had a clean shutdown
    /// @param log_start_sn The checkpoint of the log start sn
    /// @param recovery_point The checkpoint of the sn at which to begin the recovery
    static std::shared_ptr<Log> create(
        const StreamShard & stream_shard,
        const fs::path & log_dir,
        LogConfigPtr log_config,
        bool had_clean_shutdown,
        int64_t log_start_sn,
        int64_t recovery_point,
        std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler,
        std::shared_ptr<ThreadPool> adhoc_scheduler,
        TailCachePtr cache_);

    /// @param log_start_sn_ The earliest sn allowed to be exposed to NativeLog client
    /// @param loglet_ The corresponding loglet
    Log(int64_t log_start_sn_, LogletPtr loglet_, TailCachePtr cache_, Poco::Logger * logger_);

    /// Append this message set to the active segment of the Loglet, assigning sn and shard leader epochs
    /// @param record The record to append
    /// @return Information about the appended record
    LogAppendDescription append(RecordPtr & record);

    /// Read messages from the log
    /// @param sn The sequence number to begin reading at. -1 means latest, -2 means earliest
    /// @param max_size The maximum number of bytes to read
    /// @param max_wait_ms The maximum time to wait in milliseconds for new records
    /// @param position File position for sn if set
    /// @param isolation The fetch isolation, which controls the maximum sn allowed to read
    /// @return The fetch data information including fetch starting sn metadata and message read
    FetchDataDescription fetch(
        int64_t sn,
        uint64_t max_size,
        int64_t max_wait_ms,
        std::optional<uint64_t> position,
        FetchIsolation isolation = FetchIsolation::FETCH_LOG_END);

    /// Truncate to this log so that it ends with the greatest sn < target_sn
    /// @return True iff target_sn < log_end_sn
    bool trim(int64_t target_sn);

    /// Translate timestamp to sn
    /// @param ts Timestamp
    /// @param append_time Is the ts append time or event time
    /// @return The very start sn for the timestamp ts
    int64_t sequenceForTimestamp(int64_t ts, bool append_time) const;

    /// Update the retention and flush settings
    /// @param flush_settings for flush settings, 'flush_ms' and 'flush_messages'
    /// @param retention_settings for retention settings, 'retention_ms' and 'retention_bytes'
    void updateConfig(const std::map<String, int32_t> & flush_settings, const std::map<String, int64_t> & retention_settings)
    {
        loglet->updateConfig(flush_settings, retention_settings);
    }

    /// Close the log. The memory mapped buffer for index files and this log will be left open
    /// until the log is deleted
    void close();

    /// Flush all local log segments
    void flush()
    {
        auto end_meta = loglet->logEndSequenceMetadata();
        flush(end_meta.record_sn);

        {
            std::scoped_lock lock{last_committed_mutex};
            if (last_committed_metadata.record_sn < end_meta.record_sn)
                last_committed_metadata = end_meta;
        }
    }

    /// Flush Loglet segments for all sns up to `sn - 1`
    /// @param sn The sequence number to flush up to (non-inclusive); the new recovery point
    void flush(int64_t sn);

    /// Update the high watermark to the new value if and only iff it is larger than the old value.
    /// It is an error to update to a value which is larger than the log end sn
    /// This method is intended to be used by the leader to update the high watermark after follower
    /// fetch sns have been updated
    /// @return the old high watermark, if updated by the new value
    std::optional<LogSequenceMetadata> maybeIncrementHighWatermark(const LogSequenceMetadata & new_high_watermark);

    int64_t logStartSequence() const { return log_start_sn; }

    /// API calls forwarding to Loglet
    int64_t logEndSequence() const { return loglet->logEndSequence(); }

    size_t size() const { return loglet->size(); }

    const fs::path & parentDir() const { return loglet->parentDir(); }
    const fs::path & namespaceDir() const { return loglet->namespaceDir(); }
    const fs::path & rootDir() const { return loglet->rootDir(); }
    const fs::path & logDir() const { return loglet->logDir(); }

    void setInmemory(bool inmemory_) { inmemory = inmemory_; }
    bool isInmemory() const { return inmemory; }

private:
    inline LogAppendDescription analyzeAndValidateRecord(Record & record);
    inline void checkSize(int64_t record_size);
    inline void assignSequence(RecordPtr & record, ByteVector & byte_vec, LogAppendDescription & append_info) const;

    LogSegmentPtr maybeRoll(uint32_t records_size, const LogAppendDescription & append_info);
    LogSegmentPtr rollWithoutLock(std::optional<int64_t> expected_next_sn);

    /// Completely delete the log directory and all contents from the file system with no delay
    void remove();

    /// Rename the directory of the Loglet
    void renameDir(const std::string & name);

    /// Delete all data in the log and start at the new sn
    void trimFullyAndStartAt(int64_t new_sn);

    /// @return true if there is new change and need checkpoint it, otherwise return false
    bool needCheckpointRecoveryPoint() const { return loglet->needCheckpointRecoveryPoint(); }

    void beginCheckpointRecoveryPoint(int64_t recovery_point_checkpoint_)
    {
        loglet->beginCheckpointRecoveryPoint(recovery_point_checkpoint_);
    }

    void endCheckpointRecoveryPoint() { loglet->endCheckpointRecoveryPoint(); }

    /// @return true if there is new change and need checkpoint it, otherwise return false
    bool needCheckpointStartSequence() const
    {
        /// auto base_sn = firstSegment()->baseSequence();
        return log_start_sn > log_start_sn_checkpoint;
    }

    void beginCheckpointStartSequence(int64_t log_start_sn_checkpoint_) { log_start_sn_checkpoint_prepare = log_start_sn_checkpoint_; }
    void endCheckpointStartSequence() { log_start_sn_checkpoint = log_start_sn_checkpoint_prepare; }

    void updateLogStartSequence(int64_t log_start_sn_);

    void maybeIncrementFirstUnstableSequence();
    void maybeIncrementFirstUnstableSequenceWithoutLock();

    /// Increment the log start sequence if the provided sequence is larger
    /// If the log start sequence changed, then this method also update a few key sequence
    /// such that `log_start_sn <= log_stable_sn <= high_watermark`. The leader
    /// epoch cache is also updated such that all of sequences referenced in that component
    /// point to valid sequence in this log
    /// @throws OffsetOutOfRangeException if the log start sequence is greater than the
    /// high watermark
    /// @return true if the log start sequence was updated; otherwise false
    bool maybeIncrementLogStartSequence(int64_t new_log_start_sn, const std::string & reason);

    inline void checkLogStartSequence(int64_t sn) const;
    LogSequenceMetadata convertToSequenceMetadataOrThrow(int64_t sn) const;

    //    int64_t highWatermark() const
    //    {
    //        std::scoped_lock lock(lmutex);
    //        return high_watermark_metadata.record_sn;
    //    }
    //
    //    int64_t highWatermarkWithoutLock() const { return high_watermark_metadata.record_sn; }

    /// Update the high watermark to a new sn. The new high watermark will be
    /// lower bounded by the log start sn and upper bounded by the log end sn.
    /// This is intended to be called when initializing the high watermark or when
    /// updating it on a follower after receiving a Fetch response from the leader
    /// @param hw the suggested new value for the high watermark
    /// @return the updated high watermark sn
    /// int64_t updateHighWatermark(int64_t hw);

    /// @param with_lock true if the caller holds the lock already, otherwise false
    /// int64_t updateHighWatermark(const LogSequenceMetadata & high_watermark_metadata_, bool with_lock = false);

    /// void updateHighWatermarkMetadata(const LogSequenceMetadata & new_high_watermark_metadata);
    /// inline void updateHighWatermarkMetadataWithoutLock(const LogSequenceMetadata & new_high_watermark_metadata);

    /// Get the sequence and metadata for the current high watermark. If sequence metadata is not
    /// known, it will do a lookup in the index and cache the result
    /// LogSequenceMetadata fetchHighWatermarkMetadata();
    /// inline LogSequenceMetadata fetchHighWatermarkMetadataWithoutLock();

    LogSequenceMetadata fetchLastStableMetadata() const;
    inline LogSequenceMetadata fetchLastStableMetadataWithoutLock() const;
    inline LogSequenceMetadata maxSequenceMetadata(FetchIsolation isolation, bool with_committed_lock_held = false) const;
    LogSequenceMetadata waitForMoreDataIfNeeded(int64_t & sn, int64_t max_wait_ms, FetchIsolation isolation) const;

    /// If stream deletion is enabled, delete any local log segments that either expired due to time based
    /// retention or because the log size > retention_size
    /// Whether or not deletion is enabled, delete any local log segments that are before the log start offset
    size_t deleteOldSegments();

    size_t deleteLogStartSequenceBreachedSegments();
    size_t deleteRetentionSizeBreachedSegments();
    size_t deleteRetentionTimeBreachedSegments();

    /// Delete any local log segments starting with the oldest segment and moving forward until
    /// the user-supplied predicate is false or the segment containing the current high watermark
    /// is reached. We don't delete segments with offsets at or beyond the high watermark to ensure
    /// that the log start offset can never exceed it. If the high watermark has not yet been initialized,
    /// no segments are eligible for deletion
    /// @param should_delete A function that takes in a candidate log segment and the next higher segment
    ///        if there is one and return true iff it is deletable
    /// @param reason The reason for the segment deletion
    /// @return The number of segments deleted
    size_t deleteOldSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete, const std::string & reason);

private:
    /// API calls forwarding to Loglet
    size_t numberOfSegments() const { return loglet->numberOfSegments(); }
    bool isFuture() const { return loglet->isFuture(); }
    LogSegmentPtr activeSegment() const { return loglet->activeSegment(); }
    LogSegmentPtr firstSegment() const { return loglet->firstSegment(); }
    int64_t lastFlushed() const { return loglet->lastFlushed(); }
    int64_t recoveryPoint() const { return loglet->recoveryPoint(); }
    const LogConfig & config() const { return loglet->config(); }
    const StreamShard & streamShard() const { return loglet->streamShard(); }
    std::shared_ptr<ThreadPool> adhocScheduler() const { return loglet->adhocScheduler(); }

    static StreamShard streamShardFrom(const fs::path & log_dir) { return Loglet::streamShardFrom(log_dir); }
    static std::string logFutureDirName(const StreamShard & stream_shard) { return Loglet::logFutureDirName(stream_shard); }
    static std::string logDirName(const StreamShard & stream_shard) { return Loglet::logDirName(stream_shard); }
    static std::string logDeleteDirName(const StreamShard & stream_shard) { return Loglet::logDeleteDirName(stream_shard); }
    static int64_t sequenceFromFileName(const std::string & filename) { return Loglet::sequenceFromFileName(filename); }
    static bool isLogFile(const std::string & filename) { return Loglet::isLogFile(filename); }
    static bool isIndexFile(const std::string & filename) { return Loglet::isIndexFile(filename); }
    static fs::path logFile(const fs::path & log_dir, int64_t sn, const std::string & suffix = "")
    {
        return Loglet::logFile(log_dir, sn, suffix);
    }

    /// static const std::string & METADATA_STREAM() { return Loglet::METADATA_STREAM; }
    static const std::string & DELETE_DIR_SUFFIX() { return Loglet::DELETE_DIR_SUFFIX; }
    static const std::string & DELETED_FILE_SUFFIX() { return Loglet::DELETED_FILE_SUFFIX; }
    static const std::string & CLEANED_FILE_SUFFIX() { return Loglet::CLEANED_FILE_SUFFIX; }
    static const std::string & SWAP_FILE_SUFFIX() { return Loglet::SWAP_FILE_SUFFIX; }

private:
    friend class LogLoader;
    friend class LogManager;

private:
    /// The earliest sn allowed to be exposed to NativeLog client
    /// `start_sn` can be updated by
    /// - user's DeleteRecordRequest
    /// - broker's log retention
    /// - broker's log truncation
    /// - broker's log recovery
    /// The `start_sn` is used to decide the following
    /// - Log deletion. Segment whose next_sn <= log's `start_sn` can be deleted
    ///   It may trigger log rolling if the active segment is deleted
    /// - Earliest sn of the log in response to ListSequenceRequest. To avoid sequenceOutOfRange exception after user seeks
    ///   to the earliest sn, we make sure that `start_sn <= log's high watermark`.
    std::atomic<int64_t> log_start_sn;
    /// Prepare to checkpoint this log start sn
    int64_t log_start_sn_checkpoint_prepare;
    /// Start sn which is already checkpointed. Used to avoid re-checkpointing
    std::atomic<int64_t> log_start_sn_checkpoint;

    /// A mutex that guards all modifications to the following data
    /// - call append / trim / close / renameDir / replaceSegments / deleteSegments / rollSegment / flush to the loglet
    /// - read/update high_watermark_metadata
    /// - read/update first_unstable_sn_metadata
    /// - partition metadata
    /// - update producer_state_manager
    /// - leader epoch cache
    mutable std::mutex lmutex;

    LogletPtr loglet;

    TailCachePtr cache;

    std::atomic_flag closed;

    std::atomic<bool> inmemory = false;

    /// The earliest sn which is part of an incomplete transaction. This is used to compute the
    /// last stable sn (LSN) in ReplicaManager
    std::optional<LogSequenceMetadata> first_unstable_sn_metadata;

    /// Keep track of the current high watermark in order to ensure that segments containing  sns
    /// at or above it are not eligible for deletion. This means that active segment is only eligible
    /// for deletion if the high watermark equals the log end sn (which may never happen for a shard
    /// under consistent load). This is needed to prevent log start sn which is exposed in fetch
    /// responses from getting ahead of the high watermark.
    /// LogSequenceMetadata high_watermark_metadata;

    mutable std::mutex log_end_mutex;
    mutable std::condition_variable log_end_cv;

    /// Last flushed
    mutable std::mutex last_committed_mutex;
    mutable std::condition_variable last_committed_cv;
    LogSequenceMetadata last_committed_metadata;

    std::mutex segment_delete_mutex;

    Poco::Logger * logger;
};

using LogPtr = std::shared_ptr<Log>;
}
