#include "Log.h"

#include <NativeLog/Record/Record.h>
#include <base/ClockUtils.h>
#include <base/logger_useful.h>
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

FetchDataDescription Loglet::fetch(int64_t sn, int64_t max_size, std::optional<int64_t> position) const
{
    auto next_sn{next_sn_meta};
    if (sn == next_sn.record_sn)
        /// If client request sn at log end, return its sn back and empty records
        /// which basically indicates client to wait
        return {{sn}, {}};

    auto segment = segments->floorSegment(sn);
    if (!segment)
        throw DB::Exception(DB::ErrorCodes::SEQUENCE_OUT_OF_RANGE, "Data at sequence {} doesn't exist or gets pruned", sn);

    /// Do the read on the segment with a base sn less than the target sn
    /// bug if that segment doesn't contain any messages with an sn greater than that
    /// continue to read from successive segments until we get some messages or we reach the end of the log
    while (segment)
    {
        auto fetch_info = segment->read(sn, max_size, position);
        if (fetch_info.records)
            return fetch_info;
        else
            segment = segments->higherSegment(segment->baseSequence());
    }

    return {next_sn, {}};
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
    LogSegmentPtr prev_segment;
    LogSegmentPtr target_segment;
    TimestampSequence ts_seq_res{-1, -1};

    segments->apply([ts, append_time, &ts_seq_res, &prev_segment, &target_segment](LogSegmentPtr & segment) {
        auto ts_seq{segment->getIndexes().lowerBoundSequenceForTimestamp(ts, append_time)};
        if (ts_seq.isInvalid())
        {
            /// All timestamps in index are smaller than ts
            /// or index is empty. Remember this segment
            prev_segment = segment;
            return false;
        }
        else
        {
            TimestampSequence max_etime_sn{segment->maxEventTimestampSequence()};
            TimestampSequence max_atime_sn{segment->maxAppendTimestampSequence()};
            if ((append_time && max_atime_sn.key < ts) || max_etime_sn.key < ts)
            {
                prev_segment = segment;
                return false;
            }
            else
            {
                /// We find a low bound sn
                target_segment = segment;
                ts_seq_res = ts_seq;
                return true;
            }
        }
    });

    if (ts_seq_res.isInvalid())
    {
        if (prev_segment)
        {
            if (append_time)
            {
                TimestampSequence max_atime_sn{prev_segment->maxAppendTimestampSequence()};
                if (max_atime_sn.key < ts)
                    return max_atime_sn.value + 1;
                else
                    return prev_segment->baseSequence();
            }
            else
            {
                TimestampSequence max_etime_sn{prev_segment->maxEventTimestampSequence()};
                if (max_etime_sn.key < ts)
                    return max_etime_sn.value + 1;
                else
                    return prev_segment->baseSequence();
            }
        }
    }
    else
        return ts_seq_res.value;

    return LATEST_SN;
}

LogSegmentPtr Loglet::roll(std::optional<int64_t> expected_next_sn)
{
    auto start = DB::MonotonicMilliseconds::now();

    auto new_sn = std::max(expected_next_sn.value_or(0), logEndSequence());
    auto log_file = logFile(log_dir, new_sn);
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
    updateLogEndSequence(next_sn_meta.record_sn, new_segment->baseSequence(), new_segment->size());

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

std::vector<LogSegmentPtr> Loglet::trim(int64_t target_sn)
{
    auto segments_to_delete = segments->lowerEqualSegments(target_sn);
    removeSegments(segments_to_delete, true);
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
            "Found directory {} is not in form of stream_uuid.shard or stream_uuid.shard.delete or stream_uuid.shard.future",
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

    __builtin_unreachable();
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
    removeSegments(segments_to_delete, false);
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

void Loglet::updateLogEndSequence(int64_t end_offset, int64_t segment_base_offset, int64_t segment_pos)
{
    next_sn_meta.record_sn = end_offset;
    next_sn_meta.segment_base_sn = segment_base_offset;
    next_sn_meta.file_position = segment_pos;

    /// In case recovery point is already beyond log end sn,
    /// reset it
    if (recovery_point > end_offset)
        updateRecoveryPoint(end_offset);
}

void Loglet::markFlushed(int64_t sn)
{
    if (sn > recovery_point)
    {
        updateRecoveryPoint(sn);
        last_flushed_ms = DB::UTCMilliseconds::now();
    }
}

void Loglet::removeSegments(const std::vector<LogSegmentPtr> & segments_to_delete, bool async)
{
    for (const auto & segment : segments_to_delete)
        segments->remove(segment->baseSequence());

    removeAllSegmentFiles(segments_to_delete, async);
}

void Loglet::removeAllSegmentFiles(const std::vector<LogSegmentPtr> & segments_to_delete, bool async)
{
    for (auto & segment : segments_to_delete)
        segment->changeFileSuffix("", DELETED_FILE_SUFFIX);

    if (async)
    {
        std::vector<LogSegmentPtr> copy{segments_to_delete};
        /// We will need pay close attention to the lifecycle here
        adhoc_scheduler->scheduleOrThrow([moved_segments = std::move(copy), this]() {
            LOG_INFO(logger, "Deleting segment files");
            for (auto & segment : moved_segments)
                segment->remove();
        });
    }
    else
    {
        LOG_INFO(logger, "Deleting segment files");
        for (auto & segment : segments_to_delete)
            segment->remove();
    }
}

/// Given a message sn, find its corresponding sn metadata in the log
/// If the message sn is out of range, throw an exception
LogSequenceMetadata Loglet::convertToSequenceMetadataOrThrow(int64_t sn) const
{
    /// auto fetch_data_info = read(sn, 1);
    /// return fetch_data_info.fetch_sn_metadata;
    /// FIXME
    return {sn};
}
}
