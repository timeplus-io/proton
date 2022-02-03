#include "Log.h"
#include "LogLoader.h"
#include "Loglet.h"

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
    extern const int OFFSET_OUT_OF_RANGE;
}
}

namespace nlog
{
LogPtr Log::create(
    const fs::path & log_dir,
    LogConfigPtr log_config,
    const TopicID & topic_id,
    bool had_clean_shutdown,
    int64_t log_start_offset,
    int64_t recovery_point,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler,
    std::shared_ptr<ThreadPool> adhoc_scheduler)
{
    /// Create the log directory if it doesn't exist
    fs::create_directories(log_dir);

    auto logger = &Poco::Logger::get("Log");

    auto topic_shard = Log::topicShardFrom(log_dir);
    auto segments = std::make_shared<LogSegments>(topic_shard);

    LogOffsetMetadata next_offset_meta;
    auto [new_start_offset, new_recovery_point]
        = LogLoader::load(log_dir, *log_config, had_clean_shutdown, log_start_offset, recovery_point, logger, segments, next_offset_meta);

    auto loglet
        = std::make_shared<Loglet>(log_dir, log_config, new_recovery_point, next_offset_meta, segments, scheduler, adhoc_scheduler, logger);

    return std::make_shared<Log>(new_start_offset, loglet, topic_id, logger);
}

Log::Log(int64_t log_start_offset_, LogletPtr loglet_, const TopicID & topic_id_, Poco::Logger * logger_)
    : log_start_offset(log_start_offset_)
    , log_start_offset_checkpoint(log_start_offset_)
    , loglet(std::move(loglet_))
    , topic_id(std::move(topic_id_))
    , high_watermark_metadata(log_start_offset_)
    , logger(logger_)
{
    if (!logger)
        logger = &Poco::Logger::get("Log");

    updateLogStartOffset(log_start_offset);
    maybeIncrementFirstUnstableOffset();
}

LogAppendInfo Log::append(MemoryRecords & records)
{
    auto append_info = analyzeAndValidateRecords(records);

    /// Hold the lock and insert them to the log
    {
        std::lock_guard guard{lmutex};
        validateAndAssignOffsets(records, append_info);

        auto segment = maybeRoll(records.sizeInBytes(), append_info);

        LogOffsetMetadata metadata{append_info.first_offset.message_offset, segment->baseOffset(), segment->size()};

        /// Append the records, and increment the loglet end offset immediately after the append
        loglet->append(append_info.last_offset, append_info.max_timestamp, append_info.offset_of_max_timestamp, records);

        /// Update the high watermark in case it has gotten ahead of the log end offset following a truncation
        /// or if a new segment has been rolled and the offset metadata needs to be updated
        if (highWatermark() >= logEndOffset())
            updateHighWatermarkMetadataWithoutLock(loglet->logEndOffsetMetadata());

        maybeIncrementFirstUnstableOffsetWithoutLock();
    }

    if (loglet->unflushedRecords() >= config().flush_interval)
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
LogSegmentPtr Log::maybeRoll(uint32_t records_size, LogAppendInfo & append_info)
{
    /// FIXME
    auto segment = loglet->activeSegment();
    if (segment->shouldRoll(config(), records_size))
    {
        LOG_INFO(logger, "Rolling new log segment segment_size={} segment_size_threshold={}", segment->size(), config().segment_size);

        return rollWithoutLock(append_info.first_offset.message_offset);
    }
    return segment;
}

/// Roll the local log over to a new active segment starting with the expected_next_offset when provided
/// or loglet.logEndOffset otherwise.
/// @return The newly rolled segment
LogSegmentPtr Log::rollWithoutLock(std::optional<int64_t> expected_next_offset)
{
    auto new_segment = loglet->roll(expected_next_offset);

    if (highWatermark() >= logEndOffset())
        updateHighWatermarkMetadataWithoutLock(loglet->logEndOffsetMetadata());

    auto base_offset = new_segment->baseOffset();

    /// Schedule an async flush of the old segment
    adhocScheduler()->scheduleOrThrow([base_offset, this] { flush(base_offset); });
    return new_segment;
}

/// Update the offsets for this record batch and do further validation on records including
/// - Record for compacted topics must have keys
/// - Inner record must have monotonically increasing offsets starting from 0
/// - Validate / override timestamps of the records
/// FIXME, codec, move this to sequencer ?
void Log::validateAndAssignOffsets(MemoryRecords & batch, LogAppendInfo & append_info)
{
    auto initial_offset = logEndOffset();
    append_info.first_offset = initial_offset;

    auto & records = batch.mutableRecords();
    auto compact = config().compact();

    int64_t max_batch_timestamp = -1;
    int32_t offset_of_max_batch_timestamp = -1;

    for (int32_t i = 0, n = records.size(); i < n; ++i)
    {
        const auto & record = *records.Get(i);
        validateRecord(record, i, compact, append_info);
        auto timestamp = record.timestamp();
        if (timestamp > max_batch_timestamp)
        {
            max_batch_timestamp = timestamp;
            offset_of_max_batch_timestamp = initial_offset;
        }
        ++initial_offset;
    }

    //    std::for_each(records ->begin(), records->end(), [this, compact, &append_info, &index](auto * record){
    //        validateRecord(*record, index, compact, append_info);
    //        ++index;
    //    });

    processRecordErrors(append_info.record_errors);

    if (max_batch_timestamp > append_info.max_timestamp)
    {
        append_info.max_timestamp = max_batch_timestamp;
        append_info.offset_of_max_timestamp = offset_of_max_batch_timestamp;
    }

    /// Setup base offset for this batch
    batch.setBaseOffset(initial_offset - 1);
    /// FIXME leader epoch
    batch.setShardLeaderEpoch(0);
    batch.setMaxTimestamp(max_batch_timestamp);
    auto now = DB::UTCMilliseconds::now();
    batch.setAppendTimestamp(now);
    append_info.last_offset = initial_offset - 1;
    append_info.append_timestamp = now;
}

void Log::processRecordErrors(const std::vector<RecordError> & record_errors)
{
    if (!record_errors.empty())
        /// FIXME
        throw DB::Exception(DB::ErrorCodes::INVALID_RECORD, "Invalid records found");
}

void Log::validateRecord(const Record & record, size_t index, bool compact, LogAppendInfo & append_info)
{
    /// Validate offset delta
    if (record.offset_delta() != index)
    {
        append_info.record_errors.emplace_back(
            index,
            fmt::format("Record offset {0} in record batch is not assigned correctly. Expected {0}, got {1}", index, record.offset_delta()));
        return;
    }

    /// Validate key
    if (compact && record.key() == nullptr)
    {
        append_info.record_errors.emplace_back(
            index, "Compacted topic cannot accept message without key in topic shard " + topicShard().string());
        return;
    }

    /// Validate timestamp
}

///  Validate the following
/// - CRC
/// - Message size
/// FIXME, move to Sequencer ?
LogAppendInfo Log::analyzeAndValidateRecords(MemoryRecords & batch)
{
    /// if (origin == PaxosLeader && record_batch.shard_leader_epoch() != leader_epoch)
    ///    throw DB::Exception(DB::ErrorCodes::INVALID_RECORD, "Append from Paxos leader did not set the batch epoch correctly");

    if (batch.baseOffset() != 0)
        throw DB::Exception(
            DB::ErrorCodes::INVALID_RECORD,
            "The base offset of the record batch in the append to shard {} should be 0, but it is {}",
            topicShard().string(), batch.baseOffset());

    auto batch_size = batch.sizeInBytes();
    if (batch_size > config().max_message_size)
        /// FIXME Topic stats
        throw DB::Exception(
            DB::ErrorCodes::TOO_LARGE_RECORD,
            "The record batch size in append to shard {} is {} bytes which exceeds the maximum configured value of {}",
            topicShard().string(),
            batch_size,
            config().max_message_size);

    if (batch_size > config().segment_size)
        throw DB::Exception(
            DB::ErrorCodes::TOO_LARGE_RECORD_BATCH,
            "Record batch size is {} bytes in append to shard {}, which exceeds the maximum configured segment size of {}",
            batch_size,
            topicShard().string(),
            config().segment_size);

    /// Check CRC
    if (!batch.isValid())
        /// FIXME Stats
        throw DB::Exception(DB::ErrorCodes::DATA_CORRUPTION, "Record is corrupt in topic shard {}", topicShard().string());

    /// Record version check
    if (batch.version() != RecordVersion::V1)
        throw DB::Exception(
            DB::ErrorCodes::BAD_VERSION, "Bad record batch version. Expected {} but got {}", RecordVersion::V1, batch.version());

    /// Message codec
    LogAppendInfo append_info;
    append_info.first_offset = LogOffsetMetadata(batch.baseOffset());
    append_info.last_offset = batch.lastOffset();
    append_info.last_leader_epoch = batch.shardLeaderEpoch();
    append_info.max_timestamp = batch.maxTimestamp();
    append_info.offset_of_max_timestamp = batch.baseOffset();
    append_info.append_timestamp = -1;
    append_info.log_start_offset = log_start_offset;
    append_info.valid_bytes = batch.sizeInBytes();

    return append_info;
}

FetchDataInfo Log::read(int64_t offset, int32_t max_length, FetchIsolation)
{
    int64_t start_offset = offset;
    if (offset == EARLIEST_OFFSET)
        start_offset = log_start_offset;
    else if (offset == LATEST_OFFSET)
        start_offset = logEndOffset();

    assert(offset >= 0);

    return loglet->read(start_offset, max_length);
}

bool Log::trim(int64_t target_offset)
{
    if (target_offset < 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Cannot truncate shard {} to a negative offset {}",
            loglet->topicShard().string(),
            target_offset);

    auto log_end_offset = loglet->logEndOffset();
    if (target_offset >= log_end_offset)
    {
        LOG_INFO(logger, "Truncating to {} has no effect as the largest offset in the log is {}", target_offset, log_end_offset);
        return false;
    }
    else
    {
        LOG_INFO(logger, "Truncating to offset {}", target_offset);

        std::lock_guard guard{lmutex};

        if (loglet->firstSegment()->baseOffset() > target_offset)
        {
            trimFullyAndStartAt(target_offset);
        }
        else
        {
            auto deleted_segments = loglet->trim(target_offset);
            if (highWatermark() >= log_end_offset)
                updateHighWatermark(loglet->logEndOffsetMetadata(), true);
        }
        return true;
    }
}

void Log::close()
{
    if (closed.test_and_set())
        /// Already closed
        return;

    LOG_INFO(logger, "Closing shard={} in directory={}", topicShard().string(), parentDir().c_str());

    std::lock_guard guard{lmutex};
    loglet->close();
}

void Log::flush(int64_t offset)
{
    if (offset > loglet->recoveryPoint())
    {
        LOG_DEBUG(
            logger,
            "Flushing log up to offset={} for shard={} in directory={} with unflushed={} records since last_flushed={}ms",
            offset,
            loglet->topicShard().string(),
            loglet->parentDir().c_str(),
            loglet->unflushedRecords(),
            loglet->lastFlushed());

        loglet->flush(offset);

        std::lock_guard guard{lmutex};
        loglet->markFlushed(offset);
    }
}

std::optional<LogOffsetMetadata> Log::maybeIncrementHighWatermark(const LogOffsetMetadata & new_high_watermark)
{
    if (new_high_watermark.message_offset > logEndOffset())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "High watermark {} update exceeds current log end offset {}",
            new_high_watermark.message_offset,
            logEndOffset());

    std::lock_guard guard{lmutex};

    auto old_high_watermark = fetchHighWatermarkMetadataWithoutLock();

    /// Ensure that the high watermark increases monotonically. We also update the high watermark when
    /// the new offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (old_high_watermark.message_offset < new_high_watermark.message_offset
        || (old_high_watermark.message_offset == new_high_watermark.message_offset && old_high_watermark.onOldSegment(new_high_watermark)))
    {
        updateHighWatermarkMetadataWithoutLock(new_high_watermark);
        return old_high_watermark;
    }
    return {};
}

LogOffsetMetadata Log::fetchHighWatermarkMetadata()
{
    std::lock_guard guard{lmutex};
    return fetchHighWatermarkMetadataWithoutLock();
}

LogOffsetMetadata Log::fetchHighWatermarkMetadataWithoutLock()
{
    if (high_watermark_metadata.messageOffsetOnly())
    {
        auto offset_metadata = convertToOffsetMetadataOrThrow(highWatermark());
        updateHighWatermarkMetadataWithoutLock(offset_metadata);
        return offset_metadata;
    }
    return high_watermark_metadata;
}

void Log::remove()
{
    std::lock_guard guard{lmutex};

    /// producer_expire_check.cancel();
    auto deleted_segments = loglet->removeAllSegments();
    loglet->removeEmptyDir();
}

void Log::renameDir(const std::string & name)
{
    std::lock_guard guard{lmutex};

    if (loglet->renameDir(name))
    {
        /// TODO
    }
}

void Log::trimFullyAndStartAt(int64_t new_offset)
{
    (void)new_offset;
}

void Log::updateLogStartOffset(int64_t offset)
{
    log_start_offset = offset;

    if (highWatermark() < offset)
        updateHighWatermark(offset);

    if (loglet->recoveryPoint() < offset)
        loglet->updateRecoveryPoint(offset);
}

int64_t Log::updateHighWatermark(int64_t hw)
{
    return updateHighWatermark(LogOffsetMetadata{hw});
}

int64_t Log::updateHighWatermark(const LogOffsetMetadata & high_watermark_metadata_, bool with_lock)
{
    auto end_offset_metadata{loglet->logEndOffsetMetadata()};

    LogOffsetMetadata new_high_watermark_metadata{0};
    if (new_high_watermark_metadata.message_offset < log_start_offset)
        new_high_watermark_metadata = LogOffsetMetadata{log_start_offset};
    else if (new_high_watermark_metadata.message_offset >= end_offset_metadata.message_offset)
        new_high_watermark_metadata = end_offset_metadata;
    else
        new_high_watermark_metadata = high_watermark_metadata_;

    if (with_lock)
        updateHighWatermarkMetadataWithoutLock(new_high_watermark_metadata);
    else
        updateHighWatermarkMetadata(new_high_watermark_metadata);

    return new_high_watermark_metadata.message_offset;
}

void Log::updateHighWatermarkMetadata(const LogOffsetMetadata & new_high_watermark_metadata)
{
    std::lock_guard guard{lmutex};
    updateHighWatermarkMetadataWithoutLock(new_high_watermark_metadata);
}

void Log::updateHighWatermarkMetadataWithoutLock(const LogOffsetMetadata & new_high_watermark_metadata)
{
    if (new_high_watermark_metadata.message_offset < 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "High watermark offset should be non-negative");

    if (new_high_watermark_metadata.message_offset < high_watermark_metadata.message_offset)
        LOG_WARNING(
            logger,
            "Non-monotonic update of high watermark from {} to {}",
            high_watermark_metadata.message_offset,
            new_high_watermark_metadata.message_offset);

    high_watermark_metadata = new_high_watermark_metadata;
    maybeIncrementFirstUnstableOffsetWithoutLock();

    LOG_TRACE(logger, "Setting high watermark {}", new_high_watermark_metadata.message_offset);
}

void Log::maybeIncrementFirstUnstableOffset()
{
    std::lock_guard guard{lmutex};
    maybeIncrementFirstUnstableOffsetWithoutLock();
}

void Log::maybeIncrementFirstUnstableOffsetWithoutLock()
{
    /// FIXME
}

void Log::checkLogStartOffset(int64_t offset) const
{
    if (unlikely(offset < logStartOffset()))
        throw DB::Exception(
            DB::ErrorCodes::OFFSET_OUT_OF_RANGE,
            "Received request for offset {} for shard {}, but we only have segments starting from offset {}",
            offset,
            topicShard().string(),
            logEndOffset());
}

LogOffsetMetadata Log::convertToOffsetMetadataOrThrow(int64_t offset) const
{
    checkLogStartOffset(offset);
    return loglet->convertToOffsetMetadataOrThrow(offset);
}
}
