#include "Log.h"

#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OFFSET_OUT_OF_RANGE;
    const extern int BAD_FILE_NAME;
    const extern int FILE_CANNOT_READ;
    const extern int INVALID_STATE;
    const extern int INVALID_OFFSET;
    const extern int LOG_DIR_UNAVAILABLE;
}
}

namespace nlog
{
Loglet::Loglet(
    const fs::path & log_dir_,
    LogConfigPtr log_config_,
    int64_t recovery_point_,
    const LogOffsetMetadata & next_offset_meta_,
    LogSegmentsPtr segments_,
    std::shared_ptr<DB::NLOG::BackgroundSchedulePool> scheduler_,
    std::shared_ptr<ThreadPool> adhoc_scheduler_,
    Poco::Logger * logger_)
    : log_dir(log_dir_)
    , parent_dir(log_dir.parent_path())
    , root_dir(parent_dir.parent_path())
    , topic_shard(topicShardFrom(log_dir_))
    , log_config(std::move(log_config_))
    , recovery_point(recovery_point_)
    , recovery_point_checkpoint(recovery_point_)
    , next_offset_meta(next_offset_meta_)
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
        "Log shard={} in dir={} starts with recovery_point={}, next_offset_meta: {}",
        topic_shard.string(),
        log_dir.c_str(),
        recovery_point,
        next_offset_meta.string());
}

FetchDataInfo Loglet::read(int64_t offset, int64_t max_size) const
{
    int64_t end_offset = next_offset_meta.message_offset;

    auto segment = segments->floorSegment(offset);

    if (offset > end_offset || !segment)
        throw DB::Exception(DB::ErrorCodes::OFFSET_OUT_OF_RANGE, "Offset {} is out of range", offset);

    /// Do the read on the segment with a base offset less than the target offset
    /// bug if that segment doesn't contain any messages with an offset greater than that
    /// continue to read from successive segments until we get some messages or we reach the end of the log
    while (segment)
    {
        auto fetch_info = segment->read(offset, max_size);
        if (fetch_info.records)
            return fetch_info;
        else
            segment = segments->higherSegment(segment->baseOffset());
    }

    return {next_offset_meta, {}};
}

void Loglet::append(int64_t last_offset, int64_t largest_timestamp, int64_t offset_of_max_timestamp, MemoryRecords & records)
{
    auto active = segments->activeSegment();
    assert(active);
    active->append(last_offset, largest_timestamp, offset_of_max_timestamp, records);

    updateLogEndOffset(last_offset + 1, active->baseOffset(), active->size());
}

LogSegmentPtr Loglet::roll(std::optional<int64_t> expected_next_offset)
{
    auto start = DB::MonotonicMilliseconds::now();

    auto new_offset = std::max(expected_next_offset.value_or(0), logEndOffset());
    auto log_file = logFile(log_dir, new_offset);
    auto active_segment = segments->activeSegment();

    /// Segment with the same base offset shall not already exist
    assert(!segments->contains(new_offset));
    /// new_offset shall have larger offset the active segment's base offset
    assert(active_segment == nullptr || new_offset > active_segment->baseOffset());

    if (active_segment)
        active_segment->turnInactive();

    auto new_segment = std::make_shared<LogSegment>(log_dir, new_offset, config(), logger);
    segments->add(new_segment);

    /// We need to update the segment base offset and append position data of the metadata when
    /// log rolls. The next offset should not change
    updateLogEndOffset(next_offset_meta.message_offset, new_segment->baseOffset(), new_segment->size());

    LOG_INFO(logger, "Rolled new log segment at offset {} in {}ms", new_offset, DB::MonotonicMilliseconds::now() - start);
    return new_segment;
}

void Loglet::flush(int64_t offset)
{
    auto segments_to_flush = segments->values(recovery_point, offset);
    bool flush_dir = false;
    for (auto & segment : segments_to_flush)
    {
        segment->flush();
        if (segment->baseOffset() >= recovery_point)
            flush_dir = true;
    }

    if (flush_dir)
        flushFile(log_dir);
}

void Loglet::close()
{
    segments->close();
}

std::vector<LogSegmentPtr> Loglet::trim(int64_t target_offset)
{
    auto segments_to_delete = segments->lowerEqualSegments(target_offset);
    removeSegments(segments_to_delete, true);
    auto active = segments->activeSegment();
    assert(active);
    active->trim(target_offset);
    updateLogEndOffset(target_offset, active->baseOffset(), active->size());
    return segments_to_delete;
}

fs::path Loglet::logFile(const fs::path & dir, int64_t offset, const std::string & suffix)
{
    auto log_file = dir / filenamePrefixFromOffset(offset);
    log_file += (LOG_FILE_SUFFIX + suffix);
    return log_file;
}

fs::path Loglet::indexFileDir(const fs::path & dir, int64_t offset, const std::string & suffix)
{
    auto index_file = dir / filenamePrefixFromOffset(offset);
    index_file += (INDEX_FILE_SUFFIX + suffix);
    return index_file;
}

std::string Loglet::logDeleteDirName(const TopicShard & topic_shard_)
{
    auto suffix = fmt::format("-{}.{}{}", topic_shard_.shard, uuidStringWithoutHyphen(), DELETE_DIR_SUFFIX);
    if (topic_shard_.topic.size() + suffix.size() <= 255)
        return topic_shard_.topic + suffix;
    else
        return topic_shard_.topic.substr(0, 255 - suffix.size()) + suffix;
}

/// Parse log dir to extract topic name and shard ID
/// There are 3 cases
/// - topic-shard
/// - topic-shard.uuid-delete
/// - topic-shard.uuid-future
/// `uuid` above doesn't contain hyphen
TopicShard Loglet::topicShardFrom(const fs::path & log_dir_)
{
    auto topic_shard_str = log_dir_.filename().string();
    auto throw_ex = [&topic_shard_str]() {
        throw DB::Exception(
            DB::ErrorCodes::BAD_FILE_NAME,
            "Found directory {} is not in form of topic-partition or topic-partition.unique_id-delete",
            topic_shard_str);
    };

    if (topic_shard_str.empty() || topic_shard_str.find('-') == std::string::npos)
        throw_ex();

    re2::RE2 * pat = nullptr;
    TopicShard topic_shard_;
    if (topic_shard_str.ends_with(DELETE_DIR_SUFFIX))
        pat = &DELETE_DIR_PATTERN;
    else if (topic_shard_str.ends_with(FUTURE_DIR_SUFFIX))
        pat = &FUTURE_DIR_PATTERN;
    else
        pat = &LOG_DIR_PATTERN;

    int32_t num_captures = pat->NumberOfCapturingGroups() + 1;
    re2::StringPiece matches[num_captures];
    if (pat->Match(topic_shard_str, 0, topic_shard_str.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
    {
        topic_shard_.topic = matches[1].ToString();
        /// Note for string_view, indexing beyond the very end throws exception, we will add one char more, hence matches[2].size() + 1
        topic_shard_.shard = DB::parseIntStrict<int32_t>(std::string_view(matches[2].data(), matches[2].size() + 1), 0, matches[2].size());
        return topic_shard_;
    }
    else
        throw_ex();

    __builtin_unreachable();
}

LeaderEpochFileCachePtr
Loglet::createLeaderEpochCache(const fs::path & log_dir, const TopicShard & topic_shard, LogDirFailureChannelPtr log_dir_failure_channel)
{
    auto checkpoint = std::make_shared<LeaderEpochCheckpointFile>(log_dir, std::move(log_dir_failure_channel));
    return std::make_shared<LeaderEpochFileCache>(topic_shard, checkpoint);
}

int64_t Loglet::offsetFromFileName(const std::string & filename)
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

void Loglet::updateLogEndOffset(int64_t end_offset, int64_t segment_base_offset, int64_t segment_pos)
{
    next_offset_meta.message_offset = end_offset;
    next_offset_meta.segment_base_offset = segment_base_offset;
    next_offset_meta.file_position = segment_pos;

    /// In case recovery point is already beyond log end offset,
    /// reset it
    if (recovery_point > end_offset)
        updateRecoveryPoint(end_offset);
}

void Loglet::markFlushed(int64_t offset)
{
    if (offset > recovery_point)
    {
        updateRecoveryPoint(offset);
        last_flushed_ms = DB::UTCMilliseconds::now();
    }
}

void Loglet::removeSegments(const std::vector<LogSegmentPtr> & segments_to_delete, bool async)
{
    for (const auto & segment : segments_to_delete)
        segments->remove(segment->baseOffset());

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

/// Given a message offset, find its corresponding offset metadata in the log
/// If the message offset is out of range, throw an exception
LogOffsetMetadata Loglet::convertToOffsetMetadataOrThrow(int64_t offset) const
{
    /// auto fetch_data_info = read(offset, 1);
    /// return fetch_data_info.fetch_offset_metadata;
    /// FIXME
    return {offset};
}
}
