#include "Log.h"
#include "LogLoader.h"
#include "Loglet.h"

#include <NativeLog/Cache/TailCache.h>
#include <NativeLog/Common/LogSequenceMetadata.h>
#include <NativeLog/Common/StreamShard.h>
#include <NativeLog/Record/Record.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_STATE;
    extern const int INVALID_RECORD;
    extern const int TOO_LARGE_RECORD;
    extern const int TOO_LARGE_RECORD_BATCH;
    extern const int DATA_CORRUPTION;
    extern const int SEQUENCE_OUT_OF_RANGE;
}
}

namespace nlog
{
LogPtr Log::create(
    const StreamShard & stream_shard,
    const fs::path & log_dir,
    LogConfigPtr log_config,
    bool had_clean_shutdown,
    int64_t log_start_sn,
    int64_t recovery_point,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler,
    std::shared_ptr<ThreadPool> adhoc_scheduler,
    TailCachePtr cache_)
{
    /// Create the log directory if it doesn't exist
    fs::create_directories(log_dir);

    auto logger = &Poco::Logger::get("Log");

    auto segments = std::make_shared<LogSegments>(stream_shard);

    LogSequenceMetadata next_sn_meta;
    auto [new_start_sn, new_recovery_point]
        = LogLoader::load(log_dir, *log_config, had_clean_shutdown, log_start_sn, recovery_point, logger, segments, next_sn_meta);

    auto loglet = std::make_shared<Loglet>(
        stream_shard, log_dir, log_config, new_recovery_point, next_sn_meta, segments, scheduler, adhoc_scheduler, logger);

    return std::make_shared<Log>(new_start_sn, std::move(loglet), std::move(cache_), logger);
}

Log::Log(int64_t log_start_sn_, LogletPtr loglet_, TailCachePtr cache_, Poco::Logger * logger_)
    : log_start_sn(log_start_sn_)
    , log_start_sn_checkpoint(log_start_sn_)
    , loglet(std::move(loglet_))
    , cache(std::move(cache_))
    , high_watermark_metadata(log_start_sn_)
    , logger(logger_)
{
    if (!logger)
        logger = &Poco::Logger::get("Log");

    updateLogStartSequence(log_start_sn);
    maybeIncrementFirstUnstableSequence();
}

LogAppendDescription Log::append(RecordPtr & record)
{
    auto append_info = analyzeAndValidateRecord(*record);

    auto byte_vec{record->serialize()};
    checkSize(byte_vec.size());
    append_info.valid_bytes = byte_vec.size();

    /// Hold the lock and insert them to the log
    {
        std::unique_lock lock{lmutex};
        assignSequence(record, byte_vec, append_info);

        auto segment = maybeRoll(byte_vec.size(), append_info);

        LogSequenceMetadata metadata{append_info.seq_metadata.record_sn, segment->baseSequence(), segment->size()};

        /// Append the records, and increment the loglet end sn immediately after the append
        loglet->append(byte_vec, append_info);

        /// Update the high watermark in case it has gotten ahead of the log end sn following a truncation
        /// or if a new segment has been rolled and the sn metadata needs to be updated
        if (highWatermark() >= logEndSequence())
            updateHighWatermarkMetadataWithoutLock(loglet->logEndSequenceMetadata());

        maybeIncrementFirstUnstableSequenceWithoutLock();

        /// We will need hold the lmutex lock to update the cache since cache is a deque which requires sequence order
        cache->put(loglet->streamShard(), record);
    }

    log_end_sn_cv.notify_all();

    if (loglet->unflushedRecords() >= config().flush_interval_records)
        flush();

    return append_info;
}

/// Roll the log over to a new empty log segment if necessary
/// The segment will be rolled if one of the following conditions met:
/// - The segment is full
/// - The max time has elapsed since the timestamp of the first message in the segment (or since the
///   create time if the first message does not have a timestamp
/// - The index is full
/// @return The currently active segment after (perhaps) rolling to a new segment
LogSegmentPtr Log::maybeRoll(uint32_t records_size, const LogAppendDescription & append_info)
{
    const auto & conf = config();
    auto segment = loglet->activeSegment();
    if (segment->shouldRoll(conf, records_size))
    {
        LOG_INFO(logger, "Rolling new log segment segment_size={} segment_size_threshold={}", segment->size(), conf.segment_size);

        return rollWithoutLock(append_info.seq_metadata.record_sn);
    }
    return segment;
}

/// Roll the local log over to a new active segment starting with the expected_next_sn when provided
/// or loglet.logEndSequence otherwise.
/// @return The newly rolled segment
LogSegmentPtr Log::rollWithoutLock(std::optional<int64_t> expected_next_sn)
{
    auto new_segment = loglet->roll(expected_next_sn);

    if (highWatermark() >= logEndSequence())
        updateHighWatermarkMetadataWithoutLock(loglet->logEndSequenceMetadata());

    auto base_sn = new_segment->baseSequence();

    /// Schedule an async flush of the old segment.
    /// Flush can be slow and adhoc pool may be used up, we will need wait
    bool scheduled = false;
    while (!scheduled)
    {
        /// FIXME, for same log-segment, we shall avoid duplicate sync. If adhocScheduler is running out of thread slot
        /// we shall register a timer to schedule the flush later in future.
        scheduled = adhocScheduler()->trySchedule([base_sn, this] { flush(base_sn); }, 10, 50000);
        if (!scheduled)
            LOG_WARNING(logger, "adhoc pool is full, waiting for free thread slots");
    }

    return new_segment;
}

/// Update the sns for this record batch
void Log::assignSequence(RecordPtr & record, ByteVector & byte_vec, LogAppendDescription & append_info)
{
    auto initial_sn = logEndSequence();
    append_info.seq_metadata = initial_sn;
    record->setSN(initial_sn);

    auto now = DB::UTCMilliseconds::now();
    record->setAppendTime(now);
    append_info.append_timestamp = now;

    /// We will need fix the sequence number and append timestamp in the serialization
    record->deltaSerialize(byte_vec);
}

void Log::checkSize(int64_t record_size)
{
    const auto & conf = config();
    if (record_size > conf.max_record_size)
        /// FIXME Stream stats
        throw DB::Exception(
            DB::ErrorCodes::TOO_LARGE_RECORD,
            "The record batch size in append to shard {} is {} bytes which exceeds the maximum configured value of {}",
            streamShard().string(),
            record_size,
            config().max_record_size);

    if (record_size > conf.segment_size)
        throw DB::Exception(
            DB::ErrorCodes::TOO_LARGE_RECORD_BATCH,
            "Record batch size is {} bytes in append to shard {}, which exceeds the maximum configured segment size of {}",
            record_size,
            streamShard().string(),
            config().segment_size);
}

///  Validate the following
/// - CRC
/// - Message size
/// FIXME, move to Sequencer ?
LogAppendDescription Log::analyzeAndValidateRecord(Record & record)
{
    /// if (origin == PaxosLeader && record_batch.shard_leader_epoch() != leader_epoch)
    ///    throw DB::Exception(DB::ErrorCodes::INVALID_RECORD, "Append from Paxos leader did not set the batch epoch correctly");

    /// Check CRC
    if (!record.isValid())
        /// FIXME Stats
        throw DB::Exception(DB::ErrorCodes::DATA_CORRUPTION, "Record is corrupt in stream shard {}", streamShard().string());

    /// Record version check
    if (record.version() != Record::Version::V0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_VERSION, "Bad record batch version. Expected {} but got {}", Record::Version::V0, record.version());

    /// Message codec
    LogAppendDescription append_info;
    append_info.log_start_sn = log_start_sn;

    /// Assign timestamps
    auto [min_event_time, max_event_time] = record.minMaxEventTime();
    append_info.max_event_timestamp = max_event_time;
    append_info.min_event_timestamp = max_event_time;

    /// Validate key
    if (config().compact() && record.getKey().empty())
    {
        append_info.error_message = "Compacted stream cannot accept message without key in stream shard " + streamShard().string();
        append_info.error_code = DB::ErrorCodes::INVALID_RECORD;
        throw DB::Exception(DB::ErrorCodes::INVALID_RECORD, "Compacted stream cannot accept message without key in stream shard {}", streamShard().string());
    }

    return append_info;
}

FetchDataDescription Log::fetch(int64_t sn, int32_t max_length, int64_t max_wait_ms, std::optional<int64_t> position, FetchIsolation)
{
    assert(max_wait_ms >= 0);

    auto end_sn{loglet->logEndSequence()};

    if (sn == EARLIEST_SN)
        sn = log_start_sn;
    else if (sn == LATEST_SN)
        sn = end_sn;

    assert(sn >= 0);

    if (sn == end_sn)
    {
        std::unique_lock lock(lmutex);
        auto log_end_sn_changed = [this, end_sn] { return loglet->logEndSequence() > end_sn; };
        log_end_sn_cv.wait_for(lock, std::chrono::milliseconds(max_wait_ms), log_end_sn_changed);
    }

    return loglet->fetch(sn, max_length, position);
}

bool Log::trim(int64_t target_sn)
{
    if (target_sn < 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Cannot truncate shard {} to a negative sn {}", loglet->streamShard().string(), target_sn);

    auto log_end_sn = loglet->logEndSequence();
    if (target_sn >= log_end_sn)
    {
        LOG_INFO(logger, "Truncating to {} has no effect as the largest sn in the log is {}", target_sn, log_end_sn);
        return false;
    }
    else
    {
        LOG_INFO(logger, "Truncating to sn {}", target_sn);

        std::unique_lock lock{lmutex};

        if (loglet->firstSegment()->baseSequence() > target_sn)
        {
            trimFullyAndStartAt(target_sn);
        }
        else
        {
            auto deleted_segments = loglet->trim(target_sn);
            if (highWatermark() >= log_end_sn)
                updateHighWatermark(loglet->logEndSequenceMetadata(), true);
        }
        return true;
    }
}

int64_t Log::sequenceForTimestamp(int64_t ts, bool append_time) const
{
    return loglet->sequenceForTimestamp(ts, append_time);
}

void Log::close()
{
    if (closed.test_and_set())
        /// Already closed
        return;

    LOG_INFO(logger, "Closing shard={} in directory={}", streamShard().string(), parentDir().c_str());

    std::unique_lock lock{lmutex};
    loglet->close();
}

void Log::flush(int64_t sn)
{
    if (sn > loglet->recoveryPoint())
    {
        auto now = DB::MonotonicMilliseconds::now();
        loglet->flush(sn);

        LOG_DEBUG(
            logger,
            "Flushed {} records which make log flushed up to sn={} for shard={} in directory={} since last_flushed={} ms. Took {} us",
            loglet->unflushedRecords(),
            sn,
            loglet->streamShard().string(),
            loglet->parentDir().c_str(),
            loglet->lastFlushed(),
            DB::MonotonicMilliseconds::now() - now);

        std::unique_lock lock{lmutex};
        loglet->markFlushed(sn);
    }
}

std::optional<LogSequenceMetadata> Log::maybeIncrementHighWatermark(const LogSequenceMetadata & new_high_watermark)
{
    if (new_high_watermark.record_sn > logEndSequence())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "High watermark {} update exceeds current log end sn {}",
            new_high_watermark.record_sn,
            logEndSequence());

    std::unique_lock lock{lmutex};

    auto old_high_watermark = fetchHighWatermarkMetadataWithoutLock();

    /// Ensure that the high watermark increases monotonically. We also update the high watermark when
    /// the new sn metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (old_high_watermark.record_sn < new_high_watermark.record_sn
        || (old_high_watermark.record_sn == new_high_watermark.record_sn && old_high_watermark.onOldSegment(new_high_watermark)))
    {
        updateHighWatermarkMetadataWithoutLock(new_high_watermark);
        return old_high_watermark;
    }
    return {};
}

LogSequenceMetadata Log::fetchHighWatermarkMetadata()
{
    std::unique_lock lock{lmutex};
    return fetchHighWatermarkMetadataWithoutLock();
}

LogSequenceMetadata Log::fetchHighWatermarkMetadataWithoutLock()
{
    if (high_watermark_metadata.sequenceOnly())
    {
        auto sn_metadata = convertToSequenceMetadataOrThrow(highWatermark());
        updateHighWatermarkMetadataWithoutLock(sn_metadata);
        return sn_metadata;
    }
    return high_watermark_metadata;
}

void Log::remove()
{
    std::unique_lock lock{lmutex};

    /// producer_expire_check.cancel();
    auto deleted_segments = loglet->removeAllSegments();
    loglet->removeEmptyDir();
}

void Log::renameDir(const std::string & name)
{
    std::unique_lock guard{lmutex};

    if (loglet->renameDir(name))
    {
        /// TODO
    }
}

void Log::trimFullyAndStartAt(int64_t new_sn)
{
    (void)new_sn;
}

void Log::updateLogStartSequence(int64_t log_start_sn_)
{
    log_start_sn = log_start_sn_;

    if (highWatermark() < log_start_sn_)
        updateHighWatermark(log_start_sn_);

    if (loglet->recoveryPoint() < log_start_sn_)
        loglet->updateRecoveryPoint(log_start_sn_);
}

int64_t Log::updateHighWatermark(int64_t hw)
{
    return updateHighWatermark(LogSequenceMetadata{hw});
}

int64_t Log::updateHighWatermark(const LogSequenceMetadata & high_watermark_metadata_, bool with_lock)
{
    auto end_sn_metadata{loglet->logEndSequenceMetadata()};

    LogSequenceMetadata new_high_watermark_metadata{0};
    if (new_high_watermark_metadata.record_sn < log_start_sn)
        new_high_watermark_metadata = LogSequenceMetadata{log_start_sn};
    else if (new_high_watermark_metadata.record_sn >= end_sn_metadata.record_sn)
        new_high_watermark_metadata = end_sn_metadata;
    else
        new_high_watermark_metadata = high_watermark_metadata_;

    if (with_lock)
        updateHighWatermarkMetadataWithoutLock(new_high_watermark_metadata);
    else
        updateHighWatermarkMetadata(new_high_watermark_metadata);

    return new_high_watermark_metadata.record_sn;
}

void Log::updateHighWatermarkMetadata(const LogSequenceMetadata & new_high_watermark_metadata)
{
    std::unique_lock guard{lmutex};
    updateHighWatermarkMetadataWithoutLock(new_high_watermark_metadata);
}

void Log::updateHighWatermarkMetadataWithoutLock(const LogSequenceMetadata & new_high_watermark_metadata)
{
    if (new_high_watermark_metadata.record_sn < 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "High watermark sn should be non-negative");

    if (new_high_watermark_metadata.record_sn < high_watermark_metadata.record_sn)
        LOG_WARNING(
            logger,
            "Non-monotonic update of high watermark from {} to {}",
            high_watermark_metadata.record_sn,
            new_high_watermark_metadata.record_sn);

    high_watermark_metadata = new_high_watermark_metadata;
    maybeIncrementFirstUnstableSequenceWithoutLock();

    LOG_TRACE(logger, "Setting high watermark {}", new_high_watermark_metadata.record_sn);
}

void Log::maybeIncrementFirstUnstableSequence()
{
    std::unique_lock guard{lmutex};
    maybeIncrementFirstUnstableSequenceWithoutLock();
}

void Log::maybeIncrementFirstUnstableSequenceWithoutLock()
{
    /// FIXME
}

void Log::checkLogStartSequence(int64_t sn) const
{
    if (unlikely(sn < logStartSequence()))
        throw DB::Exception(
            DB::ErrorCodes::SEQUENCE_OUT_OF_RANGE,
            "Received request for sn {} for shard {}, but we only have segments starting from sn {}",
            sn,
            streamShard().string(),
            logEndSequence());
}

LogSequenceMetadata Log::convertToSequenceMetadataOrThrow(int64_t sn) const
{
    checkLogStartSequence(sn);
    return loglet->convertToSequenceMetadataOrThrow(sn);
}
}
