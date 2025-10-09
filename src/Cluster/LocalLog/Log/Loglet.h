#pragma once

#include <Cluster/Common/EpochSequence.h>
#include <Cluster/Common/StreamShard.h>
#include <Cluster/LocalLog/Base/Utils.h>
#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>
#include <Cluster/LocalLog/Indexes/SequencePositionIndex.h>
#include <Cluster/LocalLog/Indexes/TimestampSequenceIndex.h>
#include <Cluster/LocalLog/Log/LogConfig.h>
#include <Cluster/LocalLog/Log/LogSegments.h>

#include <Common/BackgroundSchedulePool.h>

#include <fmt/format.h>
#include <re2/re2.h>

namespace cluster::nlog
{
/// An append-only log which contains a list of segments
/// Loglet is not multi-thread safe
class Loglet final
{
public:
    /// \param stream_shard_ Stream name / UUID and shard number
    /// \param log_dir_ The directory in which log segments are created
    /// \param log_config_ The log configuration settings
    /// \param next_sn_meta_ The sn where the next message could be appended
    /// \param segments_ The non-empty log segments recovered from disk
    Loglet(
        const StreamShard & stream_shard_,
        const fs::path & log_dir_,
        LogConfigPtr log_config_,
        const LogSequenceMetadata & next_sn_meta_,
        LogSegmentsPtr segments_,
        TimestampSequenceIndexPtr event_ts_sn_index_,
        SequencePositionIndexPtr sn_pos_index_,
        DB::NLOG::BackgroundSchedulePool & scheduler_,
        ThreadPool & adhoc_scheduler_,
        LoggerPtr logger_);

    /// \return bytes appended and / or error code if there is
    CallResult<size_t> append(
        std::span<const cluster::EntryPtr> batch,
        const std::vector<ByteVector> & batch_data,
        size_t total_bytes,
        TimestampSequence max_event_ts);

    /// \return bytes appended and / or error code if there is
    CallResult<size_t> append(const ByteVector & entry_data, const cluster::EntryPtr & entry);

    /// \param sn The start sequence number to begin reading at
    /// \param max_size The maximum number of bytes to read
    /// \param max_sn_meta The metadata of the maximum sequence to be fetched (exclude)
    /// \param fetch_hint Provide segment's base sn and its file position for sn to be fetched
    /// \return Read log entries starting at sn passed in and up to max_sn_meta indicates.
    ///         It returns corresponding error code if something wrong happened
    CallResult<FetchResult>
    fetch(int64_t sn, uint64_t max_size, const LogSequenceMetadata & max_sn_meta, std::optional<FetchHint> fetch_hint) const;

    /// \param target_sn The sn to truncate to. After truncation, the greatest sequence in the log will be target_sn - 1
    ///                  So it trim log entries from [target_sn, ..., log_end_sn]
    /// \return DB::ErrorCode::OK when success, otherwise error code if fail
    [[nodiscard]] int trim(int64_t target_sn);

    /// \flush current active segment
    int flushActiveSegment();

    /// Total bytes on disk of all segments in this log
    size_t size() const noexcept;

    /// Update the retention and flush settings
    /// \param flush_settings for flush settings, 'flush_ms' and 'flush_messages'
    /// \param retention_settings for retention settings, 'retention_ms' and 'retention_bytes'
    void updateConfig(
        const std::unordered_map<String, int32_t> & flush_settings, const std::unordered_map<String, int64_t> & retention_settings);

    /// Translate timestamp to sn
    /// \param ts Timestamp
    /// \param append_time Is the ts append time or event time
    /// \return The sequence number of the very first log entry, starting from which
    ///         it will cover all log entries which have timestamps >= ts
    ///         If no such log entry is found, then empty std::optional is returned
    CallResult<std::optional<int64_t>> sequenceForTimestamp(int64_t ts, bool append_time) const;

    std::optional<FileLogEntries::LogSequencePosition> translateSequence(const LogSegmentPtr & segment, int64_t sn) const;

    /// Fetch log epoch sequences for rang [low_sn, high_sn)
    CallResult<std::vector<EpochSequence>> epochSequences(int64_t low_sn, int64_t high_sn) const;

public:
    static CallResult<StreamShard> streamShardFrom(const std::string & filename);

    static void removeSegmentFilesSync(
        const std::vector<LogSegmentPtr> & segments_to_delete,
        const TimestampSequenceIndexPtr & event_ts_sn_index_,
        const SequencePositionIndexPtr & sn_pos_index_,
        int64_t new_start_sn,
        std::string_view reason,
        LoggerPtr logger_);

    /// For testing etc
    /// \return encoded segment file position
    static inline uint64_t globalSegmentFilePosition(uint64_t segment_id, uint64_t file_position)
    {
        return (segment_id << 36) | file_position;
    }

    /// \return {segment_id, file_position} pair
    static inline std::pair<uint64_t, uint64_t> segmentIDAndFilePosition(uint64_t global_file_position)
    {
        return {(global_file_position & SEGMENT_NUMBER_MASK) >> 36, global_file_position & SEGMENT_FILE_POSITION_MASK};
    }

private:
    /// Construct a log file name in the given dir with the given base offset and the given suffix
    /// \param dir The directory in which the log will reside
    /// \param segment_id The id of the segment
    /// \param sn The base sn of the log file
    /// \param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
    static fs::path logFile(const fs::path & dir, uint64_t segment_id, int64_t sn, const std::string & suffix = "");

    /// Construct index file folder name in the given dir with the given suffix
    static fs::path indexFileDir(const fs::path & dir);

    /// stream_uuid.shard_id
    static std::string logDirName(const StreamShard & stream_shard_);

    /// Return a directory name to rename the log directory to for async deletion.
    /// The name will be in the following format: "stream_uuid.shard.delete"
    static std::string logDeleteDirName(const StreamShard & stream_shard_)
    {
        return logDirNameWithSuffix(stream_shard_, DELETE_DIR_SUFFIX);
    }

    /// Make log segment file name from segment id and sn bytes.
    static std::string filenamePrefixFrom(uint64_t segment_id, int64_t sn) { return fmt::format("{:08d}-{:020d}", segment_id, sn); }

    static std::string logDirNameWithSuffix(const StreamShard & stream_shard_, const std::string & suffix);

    static CallResult<std::pair<uint64_t, int64_t>> segmentIDAndSequenceFromFileName(const std::string & filename);

    static bool isIndexFile(const std::string & filename) noexcept;

    static bool isLogFile(const std::string & filename) { return filename.ends_with(LOG_FILE_SUFFIX); }

private:
    inline std::optional<uint64_t> validateFetchHint(const LogSegmentPtr & segment, int64_t sn, std::optional<FetchHint> fetch_hint) const;

    FileLogEntriesPtr doFetch(
        const LogSegmentPtr & segment,
        int64_t start_sn,
        uint64_t max_size,
        uint64_t max_position,
        std::optional<uint64_t> sn_file_position) const;

    CallResult<LogSegmentPtr> roll(std::optional<int64_t> expected_next_sn);

    int remove();

    /// The current active segment is never deletable
    std::vector<LogSegmentPtr> deletableSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete, int64_t applied_sn);

    /// Completely delete all segments with no delay
    int removeAllSegments();

    /// This method deletes the given log segments by doing the following for each of them
    /// - It removes the segment from the segment map so that it will no longer be used for reads
    /// - It renames the index and log files by appending .deleted to the respective file name
    /// - It cleans up staled entries in reverted indexes
    /// - It can either schedule an async delete operation to occur in the future or perform the deletion synchronously.
    /// Async deletion allows reads to happen concurrently without synchronization and without the possibility of
    /// physically deleting a file while it is being read
    /// \return DB::ErrorCodes::OK if success, otherwise corresponding error code
    int removeSegments(const std::vector<LogSegmentPtr> & segments_to_delete, std::string_view reason, std::optional<int64_t> new_start_sn);

    /// Perform physical deletion of the index and the log files for the given segment.
    /// Prior to the deletion, the index and log files are renamed by appending .deleted to the
    /// respective file name. Allows these files to be optionally deleted asynchronously
    /// This method assumes the file exists.
    void removeSegmentFilesAsync(std::vector<LogSegmentPtr> segments_to_delete, int64_t new_start_sn, std::string reason);

    /// Completely delete this log directory with no delay
    int removeEmptyDir();

    /// Rename the directory of the log
    /// \return DB::ErrorCodes::OK when success, otherwise corresponding error code
    int renameDir(const std::string & name);

    inline bool needFlush() const noexcept
    {
        return (log_config->flush_bytes > 0 && unflushed_bytes >= log_config->flush_bytes)
            || (log_config->flush_interval_entries > 0
                && (static_cast<uint64_t>(nextLogSequence() - last_flushed_sn) >= log_config->flush_interval_entries));
    }

    inline int64_t lastFlushedTime() const noexcept { return last_flushed_ms; }
    inline int64_t lastFlushedSequence() const noexcept { return last_flushed_sn; }

    int trimFullyAndStartAt(int64_t new_sn);

    /// The sequence number of the next message that will be appended to the log
    int64_t nextLogSequence() const;

    /// Next log sequence metadata
    LogSequenceMetadata nextLogSequenceMetadata() const noexcept;

    void updateNextLogSequence(int64_t next_sn, int64_t segment_base_sn, uint64_t segment_pos);

    LogSegmentPtr firstSegment() const noexcept { return segments->firstSegment(); }
    LogSegmentPtr activeSegment() const noexcept { return segments->activeSegment(); }

    const StreamShard & streamShard() const { return stream_shard; }
    const auto & ns() const { return stream_shard.ns; }
    const auto & name() const { return stream_shard.name; }
    auto shard() const { return stream_shard.shard(); }
    auto id() const { return stream_shard.id(); }

    LogConfigPtr config() const noexcept { return log_config; }

    const fs::path & parentDir() const noexcept { return parent_dir; }

    const fs::path & namespaceDir() const noexcept { return parent_dir; }

    const fs::path & rootDir() const noexcept { return root_dir; }

    const fs::path & logDir() const noexcept { return log_dir; }

    bool isInmemory() const noexcept { return log_config->inmemory; }

    size_t numberOfSegments() const noexcept { return segments->size(); }

    uint64_t nextSegmentID() const noexcept { return segments->activeSegment()->getID() + 1; }

    ThreadPool & adhocScheduler() const noexcept { return adhoc_scheduler; }

    friend class LogLoader;
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

    inline static auto LOG_DIR_PATTERN = re2::RE2{"^(\\S+)\\.(\\d+)$"};
    inline static auto DELETE_DIR_PATTERN = re2::RE2{"^(\\S+)\\.(\\d+)\\.delete$"};

    /// File position in inverted indexes are encoded as
    /// bits: [0-36) segment file offset / position
    /// bits: [36, 64) segment number
    inline static constexpr uint64_t SEGMENT_NUMBER_MASK = 0xff'ff'ff'f0'00'00'00'00ull;
    inline static constexpr uint64_t SEGMENT_FILE_POSITION_MASK = 0x00'00'00'0f'ff'ff'ff'ffull;

private:
    std::filesystem::path log_dir;
    std::filesystem::path parent_dir;
    std::filesystem::path root_dir;

    StreamShard stream_shard;
    LogConfigPtr log_config;

    mutable LogSegmentsPtr segments;

    mutable std::mutex next_sn_mutex;
    /// Next sequence number and physical file position for this next sequence number
    LogSequenceMetadata next_sn_meta;

    int64_t last_flushed_sn;
    int64_t unflushed_bytes = 0;
    int64_t last_flushed_ms;

    TimestampSequence max_etimestamp_and_sn_so_far;
    TimestampSequence max_atimestamp_and_sn_so_far;
    uint64_t bytes_since_last_index_entry = 0;

    TimestampSequenceIndexPtr event_ts_sn_index;
    SequencePositionIndexPtr sn_pos_index;

    /// Scheduler for scheduling repeated tasks
    DB::NLOG::BackgroundSchedulePool & scheduler;

    /// Scheduler for scheduling ad-hoc tasks which usually run once
    ThreadPool & adhoc_scheduler;

    LoggerPtr logger;
};

using LogletPtr = std::unique_ptr<Loglet>;
}
