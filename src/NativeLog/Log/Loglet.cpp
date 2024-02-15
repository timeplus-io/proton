#include "Log.h"

#include <NativeLog/Record/Record.h>
#include <base/ClockUtils.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>

namespace DB
{
namespace ErrorCodes
{
const extern int SEQUENCE_OUT_OF_RANGE;
const extern int BAD_FILE_NAME;
const extern int FILE_CANNOT_READ;
const extern int INVALID_STATE;
const extern int LOG_DIR_UNAVAILABLE;
}
}

namespace nlog
{
Loglet::Loglet(
    const StreamShard & stream_shard_,
    const fs::path & log_dir_,
    LogConfigPtr log_config_,
    int64_t recovery_point_,
    const LogSequenceMetadata & next_sn_meta_,
    LogSegmentsPtr segments_,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
    std::shared_ptr<ThreadPool> adhoc_scheduler_,
    Poco::Logger * logger_)
    : log_dir(log_dir_)
    , parent_dir(log_dir.parent_path())
    , root_dir(parent_dir.parent_path())
    , stream_shard(stream_shard_)
    , log_config(std::move(log_config_))
    , recovery_point(recovery_point_)
    , recovery_point_checkpoint(recovery_point_)
    , next_sn_meta(next_sn_meta_)
    , segments(std::move(segments_))
    , scheduler(std::move(scheduler_))
    , adhoc_scheduler(std::move(adhoc_scheduler_))
    , logger(logger_)
{
    last_flushed_ms = DB::UTCMilliseconds::now();

    if (!logger)
        logger = &Poco::Logger::get("Log");

    LOG_INFO(
        logger,
        "Log shard={} in dir={} starts with recovery_point={}, next_sn_meta: {}",
        stream_shard.string(),
        log_dir.c_str(),
        recovery_point,
        next_sn_meta.string());
}

FetchDataDescription
Loglet::fetch(int64_t sn, uint64_t max_size, const LogSequenceMetadata & max_sn_meta, std::optional<uint64_t> position) const
{
    if (sn >= max_sn_meta.record_sn)
        /// If client request sn is the max ns meta, return its sn back and empty records
        /// which basically indicates client to wait
        return {{sn}, {}};

    auto segment = segments->floorSegment(sn);
    if (!segment)
    {
        auto sn_range = segments->sequenceRange();
        throw DB::Exception(
            DB::ErrorCodes::SEQUENCE_OUT_OF_RANGE,
            "Data at sequence {} doesn't exist or gets pruned, valid sequence range=({}, {})",
            sn,
            sn_range.first,
            sn_range.second);
    }

    /// Do the read on the segment with a base sn less than the target sn
    /// bug if that segment doesn't contain any messages with an sn greater than that
    /// continue to read from successive segments until we get some messages or we reach the end of the log
    while (segment)
    {
        /// Use the max file position in the max sequence meta if it is on this segment;
        /// otherwise use the segment size as the limit
        auto max_position = max_sn_meta.segment_base_sn == segment->baseSequence() ? max_sn_meta.file_position : segment->size();
        auto fetch_info = segment->read(sn, max_size, max_position, position);
        if (fetch_info.records)
        {
            /// The target segment to serve this fetch is inactive and we are reaching the end of it
            /// set eof in the fetch info to tell caller
            if ((max_sn_meta.segment_base_sn != segment->baseSequence()) && (fetch_info.records->endPosition() == segment->size()))
                fetch_info.eof = true;

            return fetch_info;
        }
        else
            segment = segments->higherSegment(segment->baseSequence());
    }

    /// We are beyond the end of the last segment with no data fetched although the start sequence is in range.
    /// This can happen when all messages with sequence larger than start sequences have been deleted.
    /// In this case, we will return an empty records with log end sequence metadata
    return {logEndSequenceMetadata(), {}};
}

void Loglet::append(const ByteVector & record, const LogAppendDescription & append_info)
{
    auto active = segments->activeSegment();
    assert(active);
    active->append(record, append_info);

    updateLogEndSequence(append_info.seq_metadata.record_sn + 1, active->baseSequence(), active->size());
}

int64_t Loglet::sequenceForTimestamp(int64_t ts, bool append_time) const
{
    assert(ts > 0);
    LogSegmentPtr target_segment;

    /// Search the first segment whose largest timestamp >= ts
    segments->apply([ts, append_time, &target_segment](LogSegmentPtr & segment) {
        auto max_ts_sn = append_time ? segment->maxAppendTimestampSequence() : segment->maxEventTimestampSequence();
        if (max_ts_sn.key > ts)
        {
            target_segment = segment;
            return true;
        }
        return false;
    });

    if (target_segment)
        return target_segment->sequenceForTimestamp(ts, append_time);

    return LATEST_SN;
}

LogSegmentPtr Loglet::roll(std::optional<int64_t> expected_next_sn)
{
    auto start = DB::MonotonicMilliseconds::now();

    auto new_sn = std::max(expected_next_sn.value_or(0), logEndSequence());
    auto active_segment = segments->activeSegment();

    /// Segment with the same base sn shall not already exist
    assert(!segments->contains(new_sn));
    /// new_sn shall have larger sn the active segment's base sn
    assert(active_segment == nullptr || new_sn > active_segment->baseSequence());

    if (active_segment)
        active_segment->turnInactive();

    auto new_segment = std::make_shared<LogSegment>(log_dir, new_sn, config(), logger);
    segments->add(new_segment);

    /// We need to update the segment base sn and append position data of the metadata when
    /// log rolls. The next sn should not change
    updateLogEndSequence(logEndSequence(), new_segment->baseSequence(), new_segment->size());

    LOG_INFO(logger, "Rolled new log segment at sn={} in {} ms", new_sn, DB::MonotonicMilliseconds::now() - start);
    return new_segment;
}

void Loglet::flush(int64_t sn)
{
    auto segments_to_flush = segments->values(recovery_point, sn);
    bool flush_dir = false;
    for (auto & segment : segments_to_flush)
    {
        segment->flush();
        if (segment->baseSequence() >= recovery_point)
            flush_dir = true;
    }

    if (flush_dir)
        flushFile(log_dir);
}

void Loglet::close()
{
    segments->close();
}

void Loglet::updateConfig(const std::map<String, int32_t> & flush_settings, const std::map<String, int64_t> & retention_settings)
{
    /// flush settings
    for (const auto & [k, v] : flush_settings)
    {
        if (k == "flush_messages")
            log_config->flush_interval_records = v;
        else if (k == "flush_ms")
            log_config->flush_interval_ms = v;
    }

    /// retention settings
    for (const auto & [k, v] : retention_settings)
    {
        if (k == "retention_bytes")
            log_config->retention_size = v;
        else if (k == "retention_ms")
            log_config->retention_ms = v;
    }
}

std::vector<LogSegmentPtr> Loglet::trim(int64_t target_sn)
{
    auto segments_to_delete = segments->lowerEqualSegments(target_sn);
    removeSegments(segments_to_delete, true, "trim_log");
    auto active = segments->activeSegment();
    assert(active);
    active->trim(target_sn);
    updateLogEndSequence(target_sn, active->baseSequence(), active->size());
    return segments_to_delete;
}

fs::path Loglet::logFile(const fs::path & dir, int64_t sn, const std::string & suffix)
{
    auto log_file = dir / filenamePrefixFromOffset(sn);
    log_file += (LOG_FILE_SUFFIX + suffix);
    return log_file;
}

fs::path Loglet::indexFileDir(const fs::path & dir, int64_t sn, const std::string & suffix)
{
    auto index_file = dir / filenamePrefixFromOffset(sn);
    index_file += (INDEX_FILE_SUFFIX + suffix);
    return index_file;
}

std::string Loglet::logDirName(const StreamShard & stream_shard_)
{
    return fmt::format("{}.{}", DB::toString(stream_shard_.stream.id), stream_shard_.shard);
}

std::string Loglet::logDirNameWithSuffix(const StreamShard & stream_shard_, const std::string & suffix)
{
    return fmt::format("{}{}", logDirName(stream_shard_), suffix);
}

/// Parse log dir to extract stream name and shard ID
/// There are 3 cases
/// - stream_uuid.shard
/// - stream_uuid.shard.delete
/// - stream_uuid.shard.future
StreamShard Loglet::streamShardFrom(const fs::path & log_dir_)
{
    auto stream_shard_str = log_dir_.filename().string();
    auto throw_ex = [&stream_shard_str]() {
        throw DB::Exception(
            DB::ErrorCodes::BAD_FILE_NAME,
            "Found directory {} is not in form of stream_uuid.shard or stream_uuid.shard.deleted or stream_uuid.shard.future",
            stream_shard_str);
    };

    if (stream_shard_str.empty() || stream_shard_str.find('.') == std::string::npos)
        throw_ex();

    re2::RE2 * pat = nullptr;
    /// StreamShard stream_shard_;
    if (stream_shard_str.ends_with(DELETE_DIR_SUFFIX))
        pat = &DELETE_DIR_PATTERN;
    else if (stream_shard_str.ends_with(FUTURE_DIR_SUFFIX))
        pat = &FUTURE_DIR_PATTERN;
    else
        pat = &LOG_DIR_PATTERN;

    int32_t num_captures = pat->NumberOfCapturingGroups() + 1;
    re2::StringPiece matches[num_captures];
    if (pat->Match(stream_shard_str, 0, stream_shard_str.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
    {
        DB::UUID stream_id = DB::UUIDHelpers::Nil;
        auto id{matches[1].ToString()};
        DB::ReadBufferFromMemory buf(id.data(), id.size());
        auto success = DB::tryReadUUIDText(stream_id, buf);
        if (!success)
            throw_ex();

        /// Note for string_view, indexing beyond the very end throws exception, we will add one char more, hence matches[2].size() + 1
        auto shard{DB::parseIntStrict<int32_t>(std::string_view(matches[2].data(), matches[2].size() + 1), 0, matches[2].size())};
        /// FIXME stream name
        return StreamShard{"", stream_id, shard};
    }
    else
        throw_ex();

    UNREACHABLE();
}

LeaderEpochFileCachePtr
Loglet::createLeaderEpochCache(const fs::path & log_dir, const StreamShard & stream_shard, LogDirFailureChannelPtr log_dir_failure_channel)
{
    auto checkpoint = std::make_shared<LeaderEpochCheckpointFile>(log_dir, std::move(log_dir_failure_channel));
    return std::make_shared<LeaderEpochFileCache>(stream_shard, checkpoint);
}

int64_t Loglet::sequenceFromFileName(const std::string & filename)
{
    auto pos = filename.find('.');
    assert(pos != std::string::npos);
    return DB::parseIntStrict<int64_t>(filename, 0, pos);
}

bool Loglet::isIndexFile(const std::string & filename)
{
    return filename.ends_with(INDEX_FILE_SUFFIX);
}

std::vector<LogSegmentPtr> Loglet::removeAllSegments()
{
    auto segments_to_delete = segments->values();
    removeSegments(segments_to_delete, false, "delete_log");
    return segments_to_delete;
}

void Loglet::removeEmptyDir()
{
    if (!segments->empty())
        throw DB::Exception(DB::ErrorCodes::INVALID_STATE, "Cannot delete directory when {} segments are still present", segments->size());

    fs::remove_all(log_dir);
}

bool Loglet::renameDir(const std::string & name)
{
    if (segments)
        segments->close();

    auto new_log_dir{parent_dir};
    new_log_dir /= name;
    fs::rename(log_dir, new_log_dir);
    if (new_log_dir != log_dir)
    {
        log_dir = new_log_dir;
        parent_dir = new_log_dir.parent_path();
        segments->updateParentDir(new_log_dir);
        return true;
    }
    return false;
}

void Loglet::updateLogEndSequence(int64_t end_sn, int64_t segment_base_sn, uint64_t segment_pos)
{
    {
        std::scoped_lock lock(next_sn_mutex);
        next_sn_meta.record_sn = end_sn;
        next_sn_meta.segment_base_sn = segment_base_sn;
        next_sn_meta.file_position = segment_pos;
    }

    /// In case recovery point is already beyond log end sn,
    /// reset it
    if (recovery_point > end_sn)
        updateRecoveryPoint(end_sn);
}

void Loglet::markFlushed(int64_t sn)
{
    if (sn > recovery_point)
    {
        updateRecoveryPoint(sn);
        last_flushed_ms = DB::UTCMilliseconds::now();
    }
}

std::vector<LogSegmentPtr> Loglet::deletableSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete)
{
    std::vector<LogSegmentPtr> deletable;
    LogSegmentPtr current_segment;

    segments->apply([&](LogSegmentPtr & segment) {
        if (!current_segment)
        {
            current_segment = segment;
        }
        else if (should_delete(current_segment, segment))
        {
            deletable.push_back(current_segment);
            current_segment = segment;
        }
        return false;
    });

    return deletable;
}

void Loglet::removeSegments(const std::vector<LogSegmentPtr> & segments_to_delete, bool async, const std::string & reason)
{
    for (const auto & segment : segments_to_delete)
        segments->remove(segment->baseSequence());

    removeSegmentFiles(segments_to_delete, async, reason);
}

void Loglet::removeSegmentFiles(
    const std::vector<LogSegmentPtr> & segments_to_delete,
    bool async,
    const std::string & reason,
    std::shared_ptr<ThreadPool> adhoc_scheduler_,
    Poco::Logger * logger_)
{
    for (const auto & segment : segments_to_delete)
        segment->changeFileSuffix(DELETED_FILE_SUFFIX);

    if (async)
    {
        std::vector<LogSegmentPtr> copy{segments_to_delete};
        /// We will need pay close attention to the lifecycle here
        adhoc_scheduler_->scheduleOrThrow([moved_segments = std::move(copy), my_reason = reason, logger_]() {
            for (const auto & segment : moved_segments)
            {
                LOG_INFO(
                    logger_,
                    "Deleting segment file={} base_sequence={} reason={}",
                    segment->filename().c_str(),
                    segment->baseSequence(),
                    my_reason);
                segment->remove();
            }
        });
    }
    else
    {
        for (const auto & segment : segments_to_delete)
        {
            LOG_INFO(
                logger_,
                "Deleting segment file={} base_sequence={} reason={}",
                segment->filename().c_str(),
                segment->baseSequence(),
                reason);
            segment->remove();
        }
    }
}

/// Given a message sn, find its corresponding sn metadata in the log
/// If the message sn is out of range, throw an exception
LogSequenceMetadata Loglet::convertToSequenceMetadataOrThrow(int64_t sn) const
{
    auto fetch_data_info{fetch(sn, 1, logEndSequenceMetadata(), {})};
    return fetch_data_info.fetch_sn_metadata;
}

int64_t Loglet::logEndSequence() const
{
    std::scoped_lock lock(next_sn_mutex);
    return next_sn_meta.record_sn;
}

LogSequenceMetadata Loglet::logEndSequenceMetadata() const
{
    std::scoped_lock lock(next_sn_mutex);
    return next_sn_meta;
}

size_t Loglet::size() const
{
    size_t total_bytes = 0;
    segments->apply([&](LogSegmentPtr & segment) {
        total_bytes += segment->size();
        return false;
    });
    return total_bytes;
}

}
