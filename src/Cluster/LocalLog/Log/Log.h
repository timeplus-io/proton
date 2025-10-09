#pragma once

#include <Cluster/LocalLog/Log/Loglet.h>

#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/CommittedSequences.h>
#include <Cluster/Common/FetchIsolation.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/LocalLog/Log/LogEntryCache.h>

#include <absl/container/flat_hash_map.h>

#include <span>

namespace cluster
{ 

namespace nlog
{
class EpochSequenceIndex;
using EpochSequenceIndexPtr = std::unique_ptr<EpochSequenceIndex>;

class Log;
using LogPtr = std::shared_ptr<Log>;

/// A log which presents a surface wrapper of Loglet to simplify Loglet's logic. Log is supposed to more public
/// facing than Loglet and provides higher APIs.
/// Log is multi-thread safe
/// The physical directory structure of a log typically looks like
/// `log_dir` (example: /var/lib/proton/nativelog)
///    | -- stream-shard-0-dir (example: test-0)
///    | -- stream-shard-1-dir (example: test-1)
///    | -- stream-shard-2-dir (example: test-2)
///           |
///           | -- segment-id_base-sn.log (example: 00000001-00000000000000000000.log)
///           | -- epoch-sn.idx
///           | -- sn-pos.idx
///           | -- ts-sn.idx
///           | -- raft-hs.ckpt
///
/// Log and Log segments lifecycle and major operations are described briefly here
///
/// General Concepts
/// Logs are created during the creation of a stream. A stream can have multiple shards which corresponds to
/// multiple NativeLog logs. Each log can have multiple segments which usually have a bounded size like typical 4GB and is append-only.
/// Segments roll when it reaches threshold size. Once a segment rolls, it becomes immutable unless garbage collected.
/// There is always an active segment for each log which receives append request. For sure, the active segment like rolled ones can
/// serve fetch requests.
///
/// Append
/// Append is always appending to the active segment file's tail and also build `sn-pos.index`, `ts-sn.index` and `epoch-pos.index` when
/// the configurable threshold reaches like bytes reached since last indexing. These indexes are used during fetch, recovery, data
/// replication etc scenarios.
///
/// Fetch
/// Fetch on the other hand can start anywhere in a segment. So fetch can be a bit more challenging since we will need find the data offset
/// in a segment file to fetch. For example, we like fetch entries starting from sequence number N, how can we find out which segment to
/// fetch and start from which offset ? `sn-pos.index` is created for this fast lookup.
/// What `sn-pos.index` does is giving an `sn`, returning an "earliest" segment and segment offset starting from which,
/// we can find the exact offset we like to read. In general scenario, `sn-pos.index` won't tell you the exact offset, but close enough one
/// to read from since `sn-pos.index` is sparse. To find the exact offset to read, we will need reading the entries starting the offset
/// `sn-pos.index` returns. For example, to fetch entries starting from 100, `sn-pos.index` first give us sn=84, segment_id=1, segment_offset=1248,
/// then we need read entry by entry starting from segment 1 and offset 1248 to get entry with sn=100, which is 16 more entry to consume.
/// This may be a bit slow, yes, it is. But sn to segment offset lookup usually doesn't happen frequently and we have some optimization on this
/// as well to avoid the lookup. The optimization is ping-pong segment offset from server to client and then client back to server.
/// For example, the initial fetch starts from sn=100, and we do the initial translation to find out the segment and segment offset for sn=100,
/// which is segment 1 and segment offset 1248 by reusing the above example, and we read 10 entries which have total 1000 bytes in this fetch.
/// Before returning to client, we calculate the segment offset for the next read which is next sn=111 and the corresponding segment offset will
/// be 1248 + 1000 = 2248 and return this information to client. Client does this in a cooperative way to passed back the segment offset in the
/// fetch request. Since in most use cases we covered, we didn't rewind the offset in the middle, this optimization is very effective.
/// There indeed introduces some `security hole` since we rely on "good" clients. If a bad client pass in a offset which is not at entry boundary,
/// We will read partial / corrupted entries. We have code in place to detect these bad scenarios as early as possible. We may need more.
///
/// Truncation / trim
/// Truncation can happen at log tail and at log head. Tail truncation is the truncating some entries at the tail of a log.
/// Truncating at head happens in retention / garbage collection scenario which are covered in "Retentions / Garbage Collection` section.
/// There are usually 2 cases on tail truncations:
/// 1) In raft (distributed) env, the log entries are not committed and after a new leader election, there are log entries diverge from
///    the leader, we will need truncate the tailing log entries to match the log prefix of the leader and then accept new appends from
///    the leader starting from the truncated sequence.
/// 2) During recovery / bootstrap, tail log entries are corrupted.
///
/// Retention / Garbage Collection (trimHead)
/// Garbage collection can happen when size or time TTL or both reach or it happens in an automatically way which means if some entries in
/// the logs which are "determined" not required anymore, they are automatically GCed.
/// No matter which scenario triggers a GC, the current active log segment is always not GCed or is truncated at head because of race
/// condition and to handle this race condition, it introduces unnecessary complexity / performance degradation. So there may be some
/// disk waste. On mitigation could be during start of the Proton, we always roll a new segment, then the rolled segment (which doesn't
/// reach the segment size threshold) can be a candidate of GC. We don't think this mitigation is necessary for now.
///
/// There are two most major caveats when dealing Append, Fetch, Truncation and Retention: they are race condition and storage consistency.
/// 1. Race condition
///    In distributed env, "Append / Truncation" are both operating at the tail of the log. In a distributed env, they both
///    happen in single Raft thread. So these 2 don't have race. Retention on the other hand is currently scheduled in a different thread
///    which periodically checks if any retention threshold reaches, and do the garbage collection at the head of a log. During the GC of data,
///    it also needs GC the indexes (sn-pos.index, ts-sn.index, epoch-sn.index etc). "Append / Truncation" doesn't have race with "Retention"
///    on data logs but have race condition on the indexes which are not multiple thread safe. We will need apply some locking there.
///
///    The water is more muddy when Fetch request chimes in. Fetch request has race with `Append / Truncation` on the tail of the log since
///    tail of the log is for read and write.
///    Fetch request has race with `Retention` on the head of the log since `Retention` delete the head segments of the log, but `Fetch`
///    may read them and the indexes.
///
/// 2. Storage Consistency
///    There are 2 levels of consistency: 1) local log in one node. 2) log replicas in a Raft Group.
///    Local log has data log entries and the indexes. The consistency in this level basically refers to the consistency between
///    log entries and indexes. Because of having truncation (trim) / retention (trimHead), and partial commits, there may be
///    inconsistencies between data and its indexes (very common problem as in database).
///
class Log final
{
public:
    /// \param stream_shard Stream name / UUID and shard number
    /// \param log_dir The directory from which log segments need to be loaded
    /// \param log_config The configuration settings for the log being loaded
    /// \param had_clean_shutdown The boolean flag to indicate whether the associated log previously had a clean shutdown
    static LogPtr create(
        const StreamShard & stream_shard,
        const fs::path & log_dir,
        LogConfigPtr log_config,
        bool had_clean_shutdown,
        DB::NLOG::BackgroundSchedulePool & scheduler,
        ThreadPool & adhoc_scheduler);

    /// \param log_start_sn_ The earliest sn allowed to be exposed to NativeLog client
    /// \param loglet_ The corresponding loglet
    Log(int64_t log_start_sn_, EpochSequenceIndexPtr leader_epoch_index_, LogletPtr loglet_, LoggerPtr logger_);

    ~Log();

    void shutdown();
    void restart();

    /// Append this message set to the active segment of the Loglet
    /// \param entries The entries to append
    /// \return total bytes appended and / or error code (please note partial write is still
    ///         possible because of crash / signal or disk full etc
    /// Please note after append, the ownership of entries shall be moved to Log and readonly and no other
    /// references shall update the entries since internally we cache the entries
    CallResult<size_t> append(const cluster::EntryPtrs & entries);

    /// Append this message set to the active segment of the Loglet
    /// This function is dedicated for local use and
    /// it will assign epoch / sequence id
    /// \param entry The entry to append
    /// \return the sequence number or error code
    /// Please note after append, the ownership of entries shall be moved to Log and readonly and no other
    /// references shall update the entries since internally we cache the entries
    CallResult<int64_t> appendLocal(cluster::EntryPtr entry);

    /// Read messages from the log
    /// \param sn The sequence number to begin reading at. -1 means latest, -2 means earliest
    /// \param max_size The maximum number of bytes to read
    /// \param max_wait_ms The maximum time to wait in milliseconds for new entries
    /// \param fetch_hint Provide segment's base sn and its file position for sn to be fetched
    /// \param isolation The fetch isolation, which controls the maximum sn allowed to read
    /// \return The fetch data information including fetch starting sn metadata and message read
    CallResult<FetchResult> fetch(
        int64_t sn,
        uint64_t max_size,
        int64_t max_wait_ms,
        std::optional<FetchHint> fetch_hint,
        FetchIsolation isolation = FetchIsolation::LogEnd) const;

    /// Read messages range [low_sn, high_sn) from the log. The log shall cover the full range of [low_sn, high_sn)
    /// otherwise it will return error
    /// \param low_sn The sequence number to begin reading at.
    /// \param high_sn The max sequence number to read exclusive.
    /// \param max_size The maximum number of bytes to read
    /// \param fetch_hint Provide segment's base sn and its file position for sn to be fetched
    /// \param isolation The fetch isolation, which controls the maximum sn allowed to read
    /// \return The fetch data information including fetch starting sn metadata and message read
    CallResult<FetchResult> fetchRange(
        int64_t low_sn,
        int64_t high_sn,
        uint64_t max_size,
        std::optional<FetchHint> fetch_hint,
        FetchIsolation isolation = FetchIsolation::LogEnd) const;

    /// Truncate log in [target_sn, ...) at the tail of the log
    /// \return DB::ErrorCode::OK trim successfully, otherwise other error code
    [[nodiscard]] int trim(int64_t target_sn);

    /// Translate timestamp to sn
    /// \param ts Timestamp
    /// \param append_time Is the ts append time or event time
    /// \return The sequence number of the very first entry, starting from which
    ///         it will cover all entries which have timestamps >= ts
    ///         If no such record is found, then the earliest sequence log_start_sn is returned
    int64_t sequenceForTimestamp(int64_t ts, bool append_time) const;

    /// Update the retention and flush settings
    /// \param flush_settings for flush settings, 'flush_ms' and 'flush_messages'
    /// \param retention_settings for retention settings, 'retention_ms' and 'retention_bytes'
    void
    updateConfig(const std::unordered_map<String, int32_t> & flush_settings, const std::unordered_map<String, int64_t> & retention_settings)
    {
        loglet->updateConfig(flush_settings, retention_settings);
    }

    auto maxEntrySize() const noexcept { return log_config->max_entry_size; }

    bool needFlush() const noexcept { return loglet->needFlush(); }

    int flush() { return loglet->flushActiveSegment(); }

    /// Update next commit sn to the new value if and only iff it is larger than the old value.
    /// It is an error to update to a value which is larger than the log end sn
    /// \param new_epoch epoch of the committed sequence
    /// \param new_committed_sn new committed sn
    /// \return the old next commit sn
    int64_t advanceCommittedSequence(uint64_t new_epoch, int64_t new_committed_sn);
    int64_t committedSequence() const { return committed_sn; }

    /// Replay the log entries to get the last \param max_entries committed sequences
    /// [..., last_committed_sn)
    CallResult<CommittedSequences> committedSequences(int64_t last_committed_sn, uint64_t max_entries) const;

    /// Get the latest epoch. If the log is empty, the latest epoch will be std::nullptr.
    std::optional<uint64_t> latestLeaderEpoch() const;

    /// Get leader epoch for sequence number sn
    std::optional<uint64_t> leaderEpochFor(int64_t sn) const;

    int64_t logStartSequence() const { return log_start_sn; }

    int64_t logFirstSegmentBaseSequence() const noexcept { return loglet->firstSegment()->baseSequence(); }
    int64_t logActiveSegmentBaseSequence() const noexcept { return loglet->activeSegment()->baseSequence(); }

    int64_t nextLogSequence() const { return loglet->nextLogSequence(); }

    LogSequenceMetadata nextLogSequenceMetadata() const { return loglet->nextLogSequenceMetadata(); }

    size_t size() const noexcept { return loglet->size(); }

    const fs::path & parentDir() const noexcept { return loglet->parentDir(); }
    const fs::path & namespaceDir() const noexcept { return loglet->namespaceDir(); }
    const fs::path & rootDir() const noexcept { return loglet->rootDir(); }
    const fs::path & logDir() const noexcept { return loglet->logDir(); }

    const StreamShard & streamShard() const { return loglet->streamShard(); }
    const auto & ns() const { return loglet->ns(); }
    const auto & name() const { return loglet->name(); }
    auto shard() const { return loglet->shard(); }
    auto id() const { return loglet->id(); }

    bool isInmemory() const { return loglet->isInmemory(); }

    /// Make it public for testing
    LogSegmentPtr activeSegment() const { return loglet->activeSegment(); }

    /// Make it public for testing
    /// If stream deletion is enabled, delete any local log segments that either expired due to time based
    /// retention or because the log size > retention_size
    /// Whether or not deletion is enabled, delete any local log segments that are before the log start offset
    size_t deleteOldSegments(int64_t applied_sn);

    int64_t appliedSequence() const
    {
        if (applied_sn_getter)
            return applied_sn_getter();

        return committed_sn;
    }

    void setAppliedSequenceGetter(std::function<int64_t()> applied_sn_getter_) { applied_sn_getter.swap(applied_sn_getter_); }

    /// Pause/resume operations for stream control
    void pause();
    void resume();
    bool isPaused() const;
    void recover();

    /// Delete any local log segments starting with the oldest segment and moving forward until
    /// the user-supplied predicate is false or the segment containing the current high watermark
    /// is reached. We don't delete segments with offsets at or beyond the high watermark to ensure
    /// that the log start offset can never exceed it. If the high watermark has not yet been initialized,
    /// no segments are eligible for deletion
    /// \param should_delete A function that takes in a candidate log segment and the next higher segment
    ///        if there is one and return true iff it is deletable
    /// \param reason The reason for the segment deletion
    /// \return The number of segments deleted
    size_t deleteOldSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete, int64_t applied_sn, std::string_view reason);

    /// For internal use
    /// Delete all data in the log and start at the new sn
    int trimFullyAndStartAt(int64_t new_sn, std::optional<EpochSequence> prev_epoch_sn);

private:
    CallResult<size_t> append(std::span<const cluster::EntryPtr> batch);

    CallResult<size_t> appendInMemory(const cluster::EntryPtrs & entries);

    CallResult<LogSegmentPtr> maybeRoll(uint32_t append_size, int64_t next_sn);

    /// Completely delete the log directory and all contents from the file system with no delay
    int remove();

    /// Rename the directory of the log
    int renameDir(const std::string & name) { return loglet->renameDir(name); }

    void updateLogStartSequence(int64_t log_start_sn_)
    {
        assert(log_start_sn_ >= log_start_sn);
        log_start_sn = log_start_sn_;
    }

    /// Increment the log start sequence if the provided sequence is larger.
    /// \return true if the log start sequence was updated; otherwise false
    bool maybeIncrementLogStartSequence(int64_t new_log_start_sn, std::string_view reason);

    inline LogSequenceMetadata nextSequenceMetadata(FetchIsolation isolation, bool with_committed_lock_held = false) const;
    inline LogSequenceMetadata nextFetchSequenceOrWait(int64_t & sn, int64_t max_wait_ms, FetchIsolation isolation) const;

    size_t deleteLogStartSequenceBreachedSegments(int64_t applied_sn);
    size_t deleteRetentionSizeBreachedSegments(int64_t applied_sn);
    size_t deleteRetentionTimeBreachedSegments(int64_t applied_sn);

private:
    /// API calls forwarding to Loglet
    size_t numberOfSegments() const noexcept { return loglet->numberOfSegments(); }
    LogSegmentPtr firstSegment() const noexcept { return loglet->firstSegment(); }
    int64_t lastFlushedTime() const noexcept { return loglet->lastFlushedTime(); }
    auto config() const noexcept { return log_config; }
    ThreadPool & adhocScheduler() const noexcept { return loglet->adhocScheduler(); }

    static auto streamShardFrom(const std::string & filename) { return Loglet::streamShardFrom(filename); }
    static std::string logDirName(const StreamShard & stream_shard) { return Loglet::logDirName(stream_shard); }
    static std::string logDeleteDirName(const StreamShard & stream_shard) { return Loglet::logDeleteDirName(stream_shard); }
    static auto segmentIDAndSequenceFromFileName(const std::string & filename)
    {
        return Loglet::segmentIDAndSequenceFromFileName(filename);
    }
    static bool isLogFile(const std::string & filename) noexcept { return Loglet::isLogFile(filename); }
    static bool isIndexFile(const std::string & filename) noexcept { return Loglet::isIndexFile(filename); }
    static fs::path logFile(const fs::path & log_dir, uint64_t segment_id, int64_t sn, const std::string & suffix = "")
    {
        return Loglet::logFile(log_dir, segment_id, sn, suffix);
    }

    static const std::string & DELETE_DIR_SUFFIX() noexcept { return Loglet::DELETE_DIR_SUFFIX; }
    static const std::string & DELETED_FILE_SUFFIX() noexcept { return Loglet::DELETED_FILE_SUFFIX; }

private:
    friend class LogLoader;
    friend class LogManager; 

private:
    LogConfigPtr log_config;
    std::atomic_flag closed;
    
    /// Pause state for stream control
    std::atomic<bool> paused{false};

    /// Log start sn can be only changed when garbage collection happened
    /// when TTL or size or both thresholds reached or log segments are
    /// garbage collected in an automatic way.
    std::atomic<int64_t> log_start_sn;

    /// Epoch index is accessed in ReplicatedLog during bootstrap
    /// or append or log replication or log segments retention etc scenarios.
    EpochSequenceIndexPtr epoch_sn_index;

    std::function<int64_t()> applied_sn_getter;

    /// A mutex that guards all modifications to the following data
    /// - call append / trim / close / renameDir / replaceSegments / deleteSegments / rollSegment / flush to the loglet
    /// - read/update committed_sn
    mutable std::mutex log_mutex;
    mutable std::condition_variable log_cv;

    LogletPtr loglet;

    /// Entries with sn which are equal or bigger than `committed_sn`
    /// are not exposed for read / fetch if the fetch isolation is committed
    /// `committed_sn` is progressed by ReplicatedLog in Raft state machine during append
    /// `committed_sn` will be read by fetchers to check if there are more log entries to fetch
    std::atomic<int64_t> committed_sn = 0;
    std::atomic<uint64_t> committed_epoch = 0;

    LogEntryCache cached;

    LoggerPtr logger;
};
}
}
