#include <Cluster/Common/Constants.h>
#include <Cluster/LocalLog/Log/Log.h>

#include <IO/ReadHelpers.h>
#include <base/ClockUtils.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <charconv>

namespace DB
{
namespace ErrorCodes
{
const extern int ATOMIC_RENAME_FAIL;
const extern int FILE_DOESNT_EXIST;
const extern int CANNOT_DELETE_DIRECTORY;
const extern int SEQUENCE_OUT_OF_RANGE;
const extern int BAD_FILE_NAME;
const extern int INVALID_STATE;
const extern int INVALID_INTEGER_STRING;
const extern int LOG_DIR_UNAVAILABLE;
const extern int CANNOT_READ_ALL_DATA;
const extern int RECORD_NOT_FOUND;
}
}

namespace cluster::nlog
{
namespace
{

template <typename Integer>
CallResult<Integer> parseInt(std::string_view s, std::string_view::size_type lpos, std::string_view::size_type rpos)
{
    if (rpos <= lpos || rpos > s.size())
        return CallResult<Integer>({}, DB::ErrorCodes::INVALID_INTEGER_STRING);

    Integer n = 0;
    /// Note for string_view, indexing beyond the very end throws exception
    auto [p, ec] = std::from_chars(&s[lpos], &s[rpos], n);
    if (ec != std::errc())
        /// Invalid
        return CallResult<Integer>({}, DB::ErrorCodes::INVALID_INTEGER_STRING);
    else if (p != &s[rpos])
        /// Partial parsed
        return CallResult<Integer>({}, DB::ErrorCodes::INVALID_INTEGER_STRING);

    return CallResult{n, DB::ErrorCodes::OK};
}
}

Loglet::Loglet(
    const StreamShard & stream_shard_,
    const fs::path & log_dir_,
    LogConfigPtr log_config_,
    const LogSequenceMetadata & next_sn_meta_,
    LogSegmentsPtr segments_,
    TimestampSequenceIndexPtr event_ts_sn_index_,
    SequencePositionIndexPtr sn_pos_index_,
    DB::NLOG::BackgroundSchedulePool & scheduler_,
    ThreadPool & adhoc_scheduler_,
    LoggerPtr logger_)
    : log_dir(log_dir_)
    , parent_dir(log_dir.parent_path())
    , root_dir(parent_dir.parent_path())
    , stream_shard(stream_shard_)
    , log_config(std::move(log_config_))
    , segments(std::move(segments_))
    , next_sn_meta(next_sn_meta_)
    , last_flushed_sn(next_sn_meta.entry_sn - 1)
    , last_flushed_ms(DB::UTCMilliseconds::now())
    , event_ts_sn_index(std::move(event_ts_sn_index_))
    , sn_pos_index(std::move(sn_pos_index_))
    , scheduler(scheduler_)
    , adhoc_scheduler(adhoc_scheduler_)
    , logger(logger_ ? logger_ : getLogger("Log"))
{
    assert(next_sn_meta.entry_sn >= Constants::LogStartSN);

    auto res = event_ts_sn_index->lastIndexedTimestampSequence();
    if (res.hasError())
        throw DB::Exception(res.error_code, "Failed to load last indexed event timestamp and sequence");

    if (res.result)
        max_etimestamp_and_sn_so_far = *res.result;

    LOG_INFO(
        logger,
        "Log {} in dir={} starts with next_sn={}, max indexed (event_timestamp, sn)=({}, {})",
        stream_shard.logNameFull(),
        log_dir.c_str(),
        next_sn_meta.entry_sn,
        max_etimestamp_and_sn_so_far.timestamp,
        max_etimestamp_and_sn_so_far.sn);
}

CallResult<size_t> Loglet::append(
    std::span<const cluster::EntryPtr> batch, const std::vector<ByteVector> & batch_data, size_t total_bytes, TimestampSequence max_event_ts)
{
    auto active = segments->activeSegment();
    assert(active);

    auto physical_position = active->size();

    auto written = active->append(batch_data, total_bytes);
    if (written.hasError())
        return written;

    if (max_event_ts.timestamp > max_etimestamp_and_sn_so_far.timestamp)
        max_etimestamp_and_sn_so_far = max_event_ts;

    unflushed_bytes += written.result;
    bytes_since_last_index_entry += written.result;
    /// We like to index physical position to sn mapping etc at the very beginning for a log segment
    /// This will make the lookup easier later
    if (bytes_since_last_index_entry >= log_config->index_interval_bytes || physical_position == 0)
    {
        /// event ts and sn position index error are soft
        {
            auto res = event_ts_sn_index->maybeIndex(max_etimestamp_and_sn_so_far);
            if (res.hasError())
                LOG_ERROR(
                    logger,
                    "Failed index event_timestamp={} sn={} mapping",
                    max_etimestamp_and_sn_so_far.timestamp,
                    max_etimestamp_and_sn_so_far.sn);
        }
        {
            auto file_position = globalSegmentFilePosition(active->getID(), physical_position);
            auto res = sn_pos_index->maybeIndex(SequencePosition{batch.front()->sn, file_position});
            if (res.hasError())
                LOG_ERROR(logger, "Failed index sn={} file_position={} mapping", batch.front()->sn, file_position);

            /// LOG_INFO(logger, "indexing ({}->0x{:x})", batch.front()->sn, file_position);
        }

        bytes_since_last_index_entry = 0;
    }

    LOG_TRACE(logger, "Loglet::append(batch): Appended batch of {} entries, sn range [{}, {}], updating next_sn to {}", 
             batch.size(), batch.front()->sn, batch.back()->sn, batch.back()->sn + 1);
    updateNextLogSequence(batch.back()->sn + 1, active->baseSequence(), active->size());

    return written;
}

CallResult<size_t> Loglet::append(const ByteVector & entry_data, const cluster::EntryPtr & entry)
{
    auto active = segments->activeSegment();
    assert(active);

    auto physical_position = active->size();
    /// if (physical_position == 0)
    ///    rolling_based_timestamp = append_info.max_event_timestamp;

    auto written = active->append(entry_data);
    if (written.hasError())
        return written;

    /// Update the in memory max timestamp and corresponding sn.
    if (entry->max_event_timestamp > max_etimestamp_and_sn_so_far.timestamp)
    {
        max_etimestamp_and_sn_so_far.timestamp = entry->max_event_timestamp;
        max_etimestamp_and_sn_so_far.sn = entry->sn;
    }

    /// if (entry->append_timestamp > max_atimestamp_and_sn_so_far.timestamp)
    /// {
    ///     max_atimestamp_and_sn_so_far.timestamp = entry->append_timestamp;
    ///     max_atimestamp_and_sn_so_far.sn = entry->sn;
    /// }

    unflushed_bytes += written.result;
    bytes_since_last_index_entry += written.result;
    if (bytes_since_last_index_entry >= log_config->index_interval_bytes || entry->sn == Constants::LogStartSN)
    {
        /// event ts and sn position index error are soft
        {
            auto res = event_ts_sn_index->maybeIndex(max_etimestamp_and_sn_so_far);
            if (res.hasError())
                LOG_ERROR(
                    logger,
                    "Failed index event_timestamp={} sn={} mapping",
                    max_etimestamp_and_sn_so_far.timestamp,
                    max_etimestamp_and_sn_so_far.sn);
        }
        {
            auto file_position = globalSegmentFilePosition(active->getID(), physical_position);
            auto res = sn_pos_index->maybeIndex(SequencePosition{entry->sn, file_position});
            if (res.hasError())
                LOG_ERROR(logger, "Failed index sn={} file_position={} mapping", entry->sn, file_position);

            /// LOG_INFO(logger, "indexing ({}->0x{:x})", entry->sn, file_position);
        }

        bytes_since_last_index_entry = 0;
    }

    LOG_DEBUG(logger, "Loglet::append(single): Appended entry with sn={}, updating next_sn to {}", 
             entry->sn, entry->sn + 1);
    updateNextLogSequence(entry->sn + 1, active->baseSequence(), active->size());

    return written;
}

/// Validate fetch hint, returns file position if fetch hint is valid
std::optional<uint64_t> Loglet::validateFetchHint(const LogSegmentPtr & segment, int64_t sn, std::optional<FetchHint> fetch_hint) const
{
    if (!fetch_hint || fetch_hint->empty())
        return {};

    auto seg_base_sn = segment->baseSequence();
    auto seg_size = segment->size();
    assert(seg_base_sn <= sn);

    if (fetch_hint->sn == sn)
    {
        if (!fetch_hint->hasFilePosition())
            return {};

        if (fetch_hint->segment_base_sn != seg_base_sn)
        {
            LOG_WARNING(
                logger,
                "Segment base sn in fetch_hint={{{}}} is not equal to target segment which has base sn={} we like to fetch from",
                fetch_hint->string(),
                seg_base_sn);
            return {};
        }

        if (fetch_hint->file_position >= seg_size)
        {
            LOG_WARNING(
                logger,
                "File position in fetch_hint={{{}}} is not equal to target segment which has base sn={} we like to fetch from",
                fetch_hint->string(),
                seg_size);
            return {};
        }

        return fetch_hint->file_position;
    }
    else
    {
        LOG_WARNING(logger, "Fetching hint sn in fetch_hint={{{}}} is not equal to sn={} to be fetched", fetch_hint->string(), sn);
        return {};
    }
}

CallResult<FetchResult>
Loglet::fetch(int64_t sn, uint64_t max_size, const LogSequenceMetadata & max_sn_meta, std::optional<FetchHint> fetch_hint) const
{
    assert(sn < max_sn_meta.entry_sn);

    auto segment = segments->floorSegment(sn);
    if (!segment)
    {
        auto sn_range = segments->sequenceRange();
        LOG_ERROR(
            logger,
            "Log entry at sequence={} doesn't exist or gets pruned, valid sequence range=({}, {})",
            sn,
            sn_range.first,
            sn_range.second);

        return CallResult<FetchResult>{DB::ErrorCodes::SEQUENCE_OUT_OF_RANGE};
    }

    auto sn_file_position = validateFetchHint(segment, sn, fetch_hint);
    auto active_base_sn = nextLogSequenceMetadata().segment_base_sn;

    /// Do the read on the segment with a base sn less than the target sn
    /// but if that segment doesn't contain any log entries with a sn greater than the target sn,
    /// continue to read from successive segments until we get some log entries
    /// or we reach the end of the log
    CallResult<FetchResult> fetch_result;

    /// Use the max file position in the max sequence meta if it is on this segment;
    /// otherwise use the segment size as the limit
    auto base_sn = segment->baseSequence();
    auto segment_size = segment->size();
    auto max_position = segment_size;

    if (max_sn_meta.segment_base_sn && max_sn_meta.segment_base_sn.value() == base_sn && max_sn_meta.file_position)
        max_position = max_sn_meta.file_position.value();

    /// We like to fix sn_file_position if clients passed in an invalid one
    /// It is possible last segment is read and moving to the next segment
    if (sn == base_sn)
        /// The very first sequence number of this segment, reset sn_file_position to the
        /// start of the segment file for safety
        sn_file_position = 0;

    /// TODO, can we read as less data as possible before max_sn_meta.sn to avoid wasting
    /// time on reading and time on parsing and then discard what we don't like to have.
    /// Range read is typical in backfill replication.
    /// Yes, we shall do a look up the last indexed position for max_sn_meta.sn to avoid
    /// the waste. FIXME
    auto file_log_entries = doFetch(segment, sn, max_size, max_position, sn_file_position);
    if (!file_log_entries)
        return {};

    auto parsed_entries = file_log_entries->deserialize(max_sn_meta.entry_sn, max_size);

    fetch_result.error_code = parsed_entries.error_code;
    fetch_result.result.entries.swap(parsed_entries.result.second);

    if (parsed_entries.hasError())
    {
        if (!fetch_result.result.empty())
        {
            assert(fetch_result.result.entries.front()->sn == sn);
            assert(fetch_result.result.entries.back()->sn < max_sn_meta.entry_sn);

            /// We like to setup sn metadata
            fetch_result.result.fetched_log_sn_metadata.entry_sn = fetch_result.result.entries.front()->sn;
            fetch_result.result.fetched_log_sn_metadata.segment_base_sn = base_sn;
            fetch_result.result.fetched_log_sn_metadata.file_position = file_log_entries->startPosition();

            fetch_result.result.fetch_hint.sn = fetch_result.result.entries.back()->sn + 1;
            fetch_result.result.fetch_hint.segment_base_sn = base_sn; /// We are still in the current segment
            fetch_result.result.fetch_hint.file_position = file_log_entries->startPosition() + parsed_entries.result.first;

            LOG_ERROR(
                logger,
                "Failed to deserialize file log entries, error={}. Returning partial read entries={} in sn range=[{}, {}], "
                "segment_base_sn={} next_file_position={}",
                parsed_entries.errorString(),
                fetch_result.result.entries.size(),
                fetch_result.result.fetched_log_sn_metadata.entry_sn,
                fetch_result.result.fetch_hint.sn,
                fetch_result.result.fetch_hint.segment_base_sn,
                fetch_result.result.fetch_hint.file_position);
        }
        else if (parsed_entries.error_code == DB::ErrorCodes::CANNOT_READ_ALL_DATA && fetch_hint)
        {
            /// CANNOT_READ_ALL_DATA usually means parse error, and if we have fetch_hint, it may be a bad hint,
            /// in this case, return empty result set and the caller is supposed to clean the fetch hint
            LOG_ERROR(logger, "Failed to parse read data to log entries, probably fetch_hint={{{}}} is stale", fetch_hint->string());
            return {};
        }

        return fetch_result;
    }

    /// When consumer tries to consume the data a producer just produced but unflushed, there
    /// is some weird behavior: the file offset etc can be un-aligned which results in
    /// starting sn is not the sn we like to fetch. If this happens, return empty fetched entries
    /// to let client retry.
    if (fetch_result.result.empty() || fetch_result.result.entries.front()->sn != sn)
    {
        LOG_WARNING(
            logger,
            "Stale fetch hint found for file position, expected_start_sn={} fetched_start_sn={} fetched_start_position={} "
            "fetched_end_position={} fetched_hint={{{}}} segment_size={} max_sn={}",
            sn,
            fetch_result.result.empty() ? 0 : fetch_result.result.entries.front()->sn,
            file_log_entries->startPosition(),
            file_log_entries->endPosition(),
            fetch_hint ? fetch_hint->string() : "",
            segment_size,
            max_sn_meta.entry_sn);
        return {};
    }

    assert(!fetch_result.result.empty());
    assert(fetch_result.result.entries.back()->sn < max_sn_meta.entry_sn);

    fetch_result.result.fetched_log_sn_metadata.entry_sn = fetch_result.result.entries.front()->sn;
    fetch_result.result.fetched_log_sn_metadata.segment_base_sn = base_sn;
    fetch_result.result.fetched_log_sn_metadata.file_position = file_log_entries->startPosition();

    fetch_result.result.fetch_hint.sn = fetch_result.result.entries.back()->sn + 1;

    if ((base_sn != active_base_sn.value() && file_log_entries->startPosition() + parsed_entries.result.first == segment_size))
    {
        /// If the target segment which serves this fetch is sealed and we are reaching
        /// the end of it, we are going to the new segment file for next sequential read.
        /// The next segment's base sn is the prev segment's last sn + 1
        fetch_result.result.fetch_hint.segment_base_sn = fetch_result.result.entries.back()->sn + 1;
        fetch_result.result.fetch_hint.file_position = 0;

        LOG_INFO(
            logger,
            "Progress fetch hint to next segment with base_sn={} because we are reaching the end of the current segment with base_sn={} "
            "size={}",
            fetch_result.result.fetch_hint.segment_base_sn,
            base_sn,
            segment_size);
    }
    else
    {
        fetch_result.result.fetch_hint.segment_base_sn = base_sn;
        /// We use parsed_entries.result.first which is parsed_bytes returned instead of file_log_entries->endPosition()
        /// since the file log entries may be partially parsed
        fetch_result.result.fetch_hint.file_position = file_log_entries->startPosition() + parsed_entries.result.first;

        //        LOG_INFO(
        //            logger,
        //            "Fetch rannge=[{}, {}), fetched range=[{}, {}], fetch_hint={{{}}} file_log_entries end_pos={} segment_size={} "
        //            "sync_size={}",
        //            sn,
        //            max_sn_meta.entry_sn,
        //            fetch_result.result.entries.front()->sn,
        //            fetch_result.result.entries.back()->sn,
        //            fetch_result.result.fetch_hint.string(),
        //            file_log_entries->endPosition(),
        //            segment_size,
        //            segment->syncSize());
    }

    return fetch_result;
}

FileLogEntriesPtr Loglet::doFetch(
    const LogSegmentPtr & segment,
    int64_t start_sn,
    uint64_t max_size,
    uint64_t max_position,
    std::optional<uint64_t> sn_file_position) const
{
    assert(max_size > 0);

    FileLogEntries::LogSequencePosition lsn{start_sn, 0, 1};

    if (!sn_file_position)
    {
        /// If sn_position is empty or beyond of the log size,
        /// we will need translate the start_sn to a physical file position
        /// otherwise we can save this translation if client pass in a file position
        /// (we trust this is a good file position)
        auto translated_lsn = translateSequence(segment, start_sn);
        if (!translated_lsn)
            return {};

        assert(translated_lsn->sn == start_sn);
        lsn = *translated_lsn;
    }
    else
    {
        /// Client passed a file position which is in the current segment file range, use it.
        /// This is an optimization which saves a file position translation
        lsn.file_position = *sn_file_position;
    }

    assert(lsn.file_position <= max_position);

    auto adjusted_max_size = std::max(max_size, lsn.entry_size);

    assert(adjusted_max_size > 0);

    auto max_to_fetch = max_position - lsn.file_position;
    if (adjusted_max_size >= max_to_fetch)
    {
        /// We will read beyond or at the max_position. If reading the single
        /// log entry at sequence start_sn will exceed max_position then we will
        /// need wait util a bigger max_position
        if (lsn.entry_size > max_to_fetch)
            return {};

        return segment->fetch(lsn.file_position, max_to_fetch);
    }
    else
    {
        /// Within the max position
        /// lowerBoundSequenceForPosition may cross the segment boundary, we will need do further check
        auto sn_pos{sn_pos_index->lowerBoundSequenceForPosition(
            globalSegmentFilePosition(segment->getID(), lsn.file_position + adjusted_max_size))};
        if (sn_pos.hasError())
        {
            LOG_ERROR(
                logger,
                "Failed to find the left lower bound sn for end_position={} start_position={} error={}",
                lsn.file_position + adjusted_max_size,
                lsn.file_position,
                sn_pos.errorString());
            return {};
        }

        if (sn_pos.result)
        {
            /// This assert may not hold true if client passed a wrong / stale file position and we are unable to
            /// validate it upfront. It is fine in release build and also logically the sole caller in Loglet
            /// currently can handle this case.
            assert(sn_pos.result->sn > start_sn);

            auto [segment_id, file_position] = segmentIDAndFilePosition(sn_pos.result->file_position);
            assert(segment_id >= segment->getID());
            if (segment_id == segment->getID())
            {
                assert(file_position >= lsn.file_position + adjusted_max_size);
                return segment->fetch(lsn.file_position, file_position - lsn.file_position);
            }

            /// If lowerBoundSequenceForPosition crosses segments, fallthrough to slice to segment end
        }
    }

    /// Slice to max
    return segment->fetch(lsn.file_position, max_to_fetch);
}

CallResult<std::optional<int64_t>> Loglet::sequenceForTimestamp(int64_t ts, bool /*append_time*/) const
{
    auto ts_sn = event_ts_sn_index->leftLowerBoundSequenceForTimestamp(ts);
    if (ts_sn.hasError())
        return CallResult{std::optional<int64_t>{}, ts_sn.error_code};

    if (ts_sn.result)
        return CallResult{std::optional<int64_t>(ts_sn.result->sn)};

    return CallResult{std::optional<int64_t>{}};
}

CallResult<LogSegmentPtr> Loglet::roll(std::optional<int64_t> expected_next_sn)
{
    /// First flush active segment
    if (auto err = flushActiveSegment(); err != DB::ErrorCodes::OK)
    {
        LOG_ERROR(
            logger,
            "Failed to roll log segment because failed to flush the current active segment, error={}",
            DB::ErrorCodes::getName(err));
        return CallResult<LogSegmentPtr>{err};
    }

    auto new_sn = std::max(expected_next_sn.value_or(0), nextLogSequence());
    auto active_segment = segments->activeSegment();

    /// Segment with the same base sn shall not already exist
    assert(!segments->contains(new_sn));
    /// new_sn shall have larger sn the active segment's base sn
    assert(new_sn > active_segment->baseSequence());

    assert(active_segment);

    /// Turn active segment to inactive and we will need commit the last inverted index entry
    /// if we didn't index them yet
    if (auto res = event_ts_sn_index->maybeIndex(max_etimestamp_and_sn_so_far); res.hasError())
        /// We can ignore this error since it is not fatal
        LOG_ERROR(
            logger,
            "Failed to index event timestamp={} sn={} mapping, error={}",
            max_etimestamp_and_sn_so_far.timestamp,
            max_etimestamp_and_sn_so_far.sn,
            res.errorString());

    /// append_ts_sn_index->maybeIndex(max_atimestamp_and_sn_so_far);

    auto new_segment
        = std::make_shared<LogSegment>(log_dir, /*segment_id_*/ active_segment->getID() + 1, /*base_sn_*/ new_sn, config(), logger);
    segments->add(new_segment);

    /// We need to update the segment base sn and append position data of the metadata when
    /// log rolls. The next sn should not change
    updateNextLogSequence(nextLogSequence(), new_segment->baseSequence(), new_segment->size());

    return CallResult{std::move(new_segment)};
}

int Loglet::flushActiveSegment()
{
    Stopwatch stopwatch;

    auto next_sn = nextLogSequence();

    auto active = segments->activeSegment();
    assert(active);
    if (auto err = active->flush(log_config->incremental_flush); err != DB::ErrorCodes::OK)
        return err;

    /// Flush dir
    /// flushFile(log_dir);

    if (auto elapsed = stopwatch.elapsedMilliseconds(); elapsed > 50)
        LOG_WARNING(
            logger,
            "Flushed {} log entries with {} bytes for {} in directory={}. Took {}ms",
            next_sn - last_flushed_sn,
            unflushed_bytes,
            stream_shard.string(),
            parent_dir.c_str(),
            elapsed);

    last_flushed_sn = next_sn;
    unflushed_bytes = 0;
    last_flushed_ms = DB::UTCMilliseconds::now();

    return DB::ErrorCodes::OK;
}

void Loglet::updateConfig(
    const std::unordered_map<String, int32_t> & flush_settings, const std::unordered_map<String, int64_t> & retention_settings)
{
    /// flush settings
    for (const auto & [k, v] : flush_settings)
    {
        if (k == "flush_messages")
            log_config->flush_interval_entries = v;
        else if (k == "flush_bytes")
            log_config->flush_bytes = v;
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

/// \trim log up to sn, a.k.a remove [sn, log_end_sn]
int Loglet::trim(int64_t target_sn)
{
    auto active_segment = segments->activeSegment();
    auto target_segment = segments->floorSegment(target_sn);
    if (!target_segment)
    {
        LOG_ERROR(
            logger,
            "Failed to truncate log. Didn't find a segment containing target trim_sn={}. Current segments base sn range=[{}, {}] "
            "next_sn={}",
            target_sn,
            segments->firstSegment()->baseSequence(),
            active_segment->baseSequence(),
            nextLogSequence());

        return DB::ErrorCodes::RECORD_NOT_FOUND;
    }

    if (target_segment->baseSequence() != active_segment->baseSequence())
        /// There is case unstable sns just rolled to a new segment
        /// ref : https://github.com/timeplus-io/proton-enterprise/issues/9787
        LOG_INFO(
            logger,
            "Trimming non-tail segment, target_segment_base_sn={} active_segment_base_sn={} target trim_sn={}",
            target_segment->baseSequence(),
            active_segment->baseSequence(),
            target_sn);

    /// We like to first translate the sn since sn_pos_index got first trimmed
    auto lsn = translateSequence(target_segment, target_sn);
    if (!lsn)
    {
        LOG_ERROR(
            logger,
            "Failed to truncate log. Didn't find file system position for target trim sn={}, current active segment has base_sn={}, "
            "next_sn={}",
            target_sn,
            target_segment->baseSequence(),
            nextLogSequence());

        return DB::ErrorCodes::RECORD_NOT_FOUND;
    }

    if (auto err = sn_pos_index->trim(target_sn); err != DB::ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to trim sn and file position mapping index, error={}", DB::ErrorCodes::getName(err));
        return err;
    }

    if (auto err = event_ts_sn_index->trim(target_sn); err != DB::ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to trim event timestamp and sn mapping index, error={}", DB::ErrorCodes::getName(err));
        return err;
    }

    /// re-evaluate max_etimestamp and max_atimestamp
    if (auto res = event_ts_sn_index->lastIndexedTimestampSequence(); !res.hasError())
    {
        if (res.result)
            max_etimestamp_and_sn_so_far = *res.result;
        else
            max_etimestamp_and_sn_so_far = TimestampSequence{};
    }
    else
    {
        LOG_ERROR(logger, "Failed to get the last indexed event timestamp and sn mapping, error={}", res.errorString());
        return res.error_code;
    }

    auto segments_to_delete = segments->upperSegments(target_sn);
    if (auto err = removeSegments(segments_to_delete, "trim_log", {}); err != DB::ErrorCodes::OK)
        return err;

    auto bytes_before_trim = target_segment->size();
    if (auto res = target_segment->trim(lsn->file_position); !res.hasError())
    {
        LOG_INFO(
            logger,
            "Trim {} bytes in range [{}, ...) on segment={}, bytes_before_trim={} bytes_after_trim={}",
            res.result,
            target_sn,
            target_segment->getID(),
            bytes_before_trim,
            target_segment->size());

        updateNextLogSequence(target_sn, target_segment->baseSequence(), target_segment->size());
        return DB::ErrorCodes::OK;
    }
    else
    {
        return res.error_code;
    }
}

int Loglet::trimFullyAndStartAt(int64_t new_sn)
{
    auto segments_to_delete = segments->allSegments();
    assert(!segments_to_delete.empty());

    /// Keep the active around first
    auto active = segments_to_delete.back();
    segments_to_delete.pop_back();

    if (auto err = removeSegments(segments_to_delete, "trim_log_fully", new_sn); err != DB::ErrorCodes::OK)
        return err;

    /// Here we like to enforce there is an active segment always even during
    /// the trim processing
    if (new_sn == active->baseSequence())
    {
        if (auto err = active->changeFileSuffix(DELETED_FILE_SUFFIX); err != DB::ErrorCodes::OK)
            return err;
    }

    /// Create the new log segment starts with new_sn
    auto new_segment = std::make_shared<LogSegment>(log_dir, /*segment_id_=*/active->getID() + 1, /*base_sn_=*/new_sn, log_config, logger);
    segments->add(std::move(new_segment));

    if (new_sn != active->baseSequence())
        segments->remove(active->baseSequence());

    /// Remove the last segment
    removeSegmentFilesAsync({std::move(active)}, new_sn, "trim_log_fully");

    /// Reset next_sn_meta
    next_sn_meta.entry_sn = new_sn;
    next_sn_meta.segment_base_sn = new_sn;
    next_sn_meta.file_position = 0;

    return DB::ErrorCodes::OK;
}

fs::path Loglet::logFile(const fs::path & dir, uint64_t segment_id, int64_t sn, const std::string & suffix)
{
    auto log_file = dir / filenamePrefixFrom(segment_id, sn);
    log_file += (LOG_FILE_SUFFIX + suffix);
    return log_file;
}

fs::path Loglet::indexFileDir(const fs::path & dir)
{
    return dir / fmt::format("inverted{}", INDEX_FILE_SUFFIX);
}

std::string Loglet::logDirName(const StreamShard & stream_shard_)
{
    return fmt::format("{}.{}", DB::toString(stream_shard_.id_shard.id), stream_shard_.id_shard.shard);
}

std::string Loglet::logDirNameWithSuffix(const StreamShard & stream_shard_, const std::string & suffix)
{
    return fmt::format("{}{}", logDirName(stream_shard_), suffix);
}

/// Parse log dir to extract stream name and shard ID
/// There are 2 cases
/// - stream_uuid.shard
/// - stream_uuid.shard.delete
CallResult<StreamShard> Loglet::streamShardFrom(const std::string & filename)
{
    if (filename.empty() || !filename.contains('.'))
        return CallResult<StreamShard>{DB::ErrorCodes::BAD_FILE_NAME};

    re2::RE2 * pat = nullptr;
    /// StreamShard stream_shard_;
    if (filename.ends_with(DELETE_DIR_SUFFIX))
        pat = &DELETE_DIR_PATTERN;
    else
        pat = &LOG_DIR_PATTERN;

    int32_t num_captures = pat->NumberOfCapturingGroups() + 1;
    re2::StringPiece matches[num_captures];
    if (pat->Match(filename, 0, filename.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
    {
        StreamID stream_id = DB::UUIDHelpers::Nil;
        auto id{matches[1]};
        DB::ReadBufferFromMemory buf(id.data(), id.size());
        if (auto success = DB::tryReadUUIDText(stream_id, buf); !success)
            return CallResult<StreamShard>{DB::ErrorCodes::BAD_FILE_NAME};

        /// Note for string_view, indexing beyond the very end throws exception, we will add one char more, hence matches[2].size() + 1
        auto res = parseInt<uint32_t>(std::string_view(matches[2].data(), matches[2].size() + 1), 0, matches[2].size());
        if (res.hasError())
            return CallResult<StreamShard>{DB::ErrorCodes::BAD_FILE_NAME};

        return CallResult{StreamShard{"", "", stream_id, res.result}};
    }
    else
    {
        return CallResult<StreamShard>{DB::ErrorCodes::BAD_FILE_NAME};
    }
}

CallResult<std::pair<uint64_t, int64_t>> Loglet::segmentIDAndSequenceFromFileName(const std::string & filename)
{
    /// [segid]-[sequence number].log
    /// 000001-00000000034.log
    auto segment_id_pos = filename.find('-');
    if (segment_id_pos == std::string::npos)
        return CallResult<std::pair<uint64_t, int64_t>>(DB::ErrorCodes::BAD_FILE_NAME);

    auto sn_pos = filename.find('.');
    if (sn_pos == std::string::npos || sn_pos <= segment_id_pos)
        return CallResult<std::pair<uint64_t, int64_t>>(DB::ErrorCodes::BAD_FILE_NAME);

    auto seg_id = parseInt<uint64_t>(filename, 0, segment_id_pos);
    if (seg_id.hasError())
        return CallResult<std::pair<uint64_t, int64_t>>(DB::ErrorCodes::BAD_FILE_NAME);

    auto seq = parseInt<int64_t>(filename, segment_id_pos + 1, sn_pos);
    if (seq.hasError())
        return CallResult<std::pair<uint64_t, int64_t>>(DB::ErrorCodes::BAD_FILE_NAME);

    return CallResult{std::pair{seg_id.result, seq.result}};
}

bool Loglet::isIndexFile(const std::string & filename) noexcept
{
    return filename.ends_with(INDEX_FILE_SUFFIX);
}

int Loglet::remove()
{
    if (auto err = removeAllSegments(); err != DB::ErrorCodes::OK)
        return err;

    if (event_ts_sn_index)
        event_ts_sn_index->close();

    if (sn_pos_index)
        sn_pos_index->close();

    return removeEmptyDir();
}

int Loglet::removeAllSegments()
{
    auto segments_to_delete = segments->allSegments();
    return removeSegments(segments_to_delete, "delete_log", nextLogSequence());
}

int Loglet::removeEmptyDir()
{
    if (!segments->empty())
        return DB::ErrorCodes::INVALID_STATE;

    std::error_code ec;
    fs::remove_all(log_dir, ec);
    if (ec.value() != 0)
    {
        LOG_ERROR(logger, "Failed to remove log_dir={}, error={}", log_dir.c_str(), ec.message());
        return DB::ErrorCodes::CANNOT_DELETE_DIRECTORY;
    }

    return DB::ErrorCodes::OK;
}

int Loglet::renameDir(const std::string & name)
{
    auto new_log_dir{parent_dir};
    new_log_dir /= name;

    std::error_code ec;
    fs::rename(log_dir, new_log_dir, ec);
    if (ec.value() != 0)
    {
        if (ec == std::errc::no_such_file_or_directory)
            return DB::ErrorCodes::FILE_DOESNT_EXIST;

        return DB::ErrorCodes::ATOMIC_RENAME_FAIL;
    }

    if (new_log_dir != log_dir)
    {
        log_dir = new_log_dir;
        parent_dir = new_log_dir.parent_path();
        segments->updateParentDir(new_log_dir);
    }

    return DB::ErrorCodes::OK;
}

void Loglet::updateNextLogSequence(int64_t next_sn, int64_t segment_base_sn, uint64_t segment_pos)
{
    assert(next_sn >= Constants::LogStartSN);
    assert(segment_base_sn >= Constants::LogStartSN);
    {
        std::scoped_lock lock(next_sn_mutex);
        auto old_sn = next_sn_meta.entry_sn;
        LOG_DEBUG(
            logger, "updateNextLogSequence: Updating next_sn from {} to {} for stream_shard={}", old_sn, next_sn, stream_shard.string());
        /// assert(next_sn_meta.entry_sn + 1 == next_sn);
        next_sn_meta.entry_sn = next_sn;
        next_sn_meta.segment_base_sn = segment_base_sn;
        next_sn_meta.file_position = segment_pos;

        last_flushed_sn = std::min(last_flushed_sn, next_sn - 1);
    }
}

std::vector<LogSegmentPtr> Loglet::deletableSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete, int64_t applied_sn)
{
    std::vector<LogSegmentPtr> deletable;
    LogSegmentPtr prev_segment;

    segments->apply([&](LogSegmentPtr & segment) {
        if (!prev_segment)
        {
            prev_segment = segment;
        }
        else if (
            /// We need make sure the next segment base sn is less than applied_sn and we choose less than
            /// instead of less equal than just in case
            (segment->baseSequence() < applied_sn) && should_delete(prev_segment, segment))
        {
            chassert(prev_segment->baseSequence() < segment->baseSequence());
            deletable.push_back(std::move(prev_segment));
            prev_segment = segment;
        }
        else
        {
            /// Stop looping
            return true;
        }
        return false;
    });

    return deletable;
}

int Loglet::removeSegments(
    const std::vector<LogSegmentPtr> & segments_to_delete, std::string_view reason, std::optional<int64_t> new_start_sn)
{
    if (segments_to_delete.empty())
        return DB::ErrorCodes::OK;

    for (const auto & segment : segments_to_delete)
        segments->remove(segment->baseSequence());

    for (const auto & segment : segments_to_delete)
    {
        if (auto err = segment->changeFileSuffix(DELETED_FILE_SUFFIX); err != DB::ErrorCodes::OK)
        {
            if (err == DB::ErrorCodes::FILE_DOESNT_EXIST)
                /// If file doesn't exist for whatever reason, continue to the next segment
                continue;
            else
                return err;
        }
    }

    if (!new_start_sn)
    {
        assert(!segments->empty());
        new_start_sn = segments->firstSegment()->baseSequence();
    }
    else
    {
        assert(segments->empty());
    }

    assert(new_start_sn);

    removeSegmentFilesAsync(segments_to_delete, *new_start_sn, std::string{reason});

    return DB::ErrorCodes::OK;
}

void Loglet::removeSegmentFilesAsync(std::vector<LogSegmentPtr> segments_to_delete, int64_t new_start_sn, std::string reason)
{
    assert(!segments_to_delete.empty());

    /// For trimHead error, we can safely ignore them since they are not fatal.
    if (auto err = event_ts_sn_index->trimHead(new_start_sn); err != DB::ErrorCodes::OK)
        LOG_ERROR(
            logger,
            "Failed to truncate event timestamp and sn mapping index to sn={}, error={}",
            new_start_sn,
            DB::ErrorCodes::getName(err));

    if (auto err = sn_pos_index->trimHead(new_start_sn); err != DB::ErrorCodes::OK)
        LOG_ERROR(
            logger, "Failed to truncate sn and file position mapping index to sn={}, error={}", new_start_sn, DB::ErrorCodes::getName(err));

    /// We will need pay close attention to the lifecycle here
    adhoc_scheduler.scheduleOrThrowOnError(
        [moved_segments = std::move(segments_to_delete), reason_ = std::move(reason), logger_ = logger]() {
            setThreadName("LogSegDeleter");
            for (const auto & segment : moved_segments)
            {
                LOG_INFO(
                    logger_,
                    "Deleting segment file={} base_sequence={} reason={}",
                    segment->filename().c_str(),
                    segment->baseSequence(),
                    reason_);

                segment->remove();
            }
        });
}

void Loglet::removeSegmentFilesSync(
    const std::vector<LogSegmentPtr> & segments_to_delete,
    const TimestampSequenceIndexPtr & event_ts_sn_index_,
    const SequencePositionIndexPtr & sn_pos_index_,
    int64_t new_start_sn,
    std::string_view reason,
    LoggerPtr logger_)
{
    if (auto err = event_ts_sn_index_->trimHead(new_start_sn); err != DB::ErrorCodes::OK)
        LOG_ERROR(
            logger_,
            "Failed to truncate event timestamp and sn mapping index to sn={}, error={}",
            new_start_sn,
            DB::ErrorCodes::getName(err));

    if (auto err = sn_pos_index_->trimHead(new_start_sn); err != DB::ErrorCodes::OK)
        LOG_ERROR(
            logger_,
            "Failed to truncate sn and file position mapping index to sn={}, error={}",
            new_start_sn,
            DB::ErrorCodes::getName(err));

    for (const auto & segment : segments_to_delete)
    {
        LOG_INFO(
            logger_, "Deleting segment file={} base_sequence={} reason={}", segment->filename().c_str(), segment->baseSequence(), reason);

        segment->remove();
    }
}

int64_t Loglet::nextLogSequence() const
{
    std::scoped_lock lock(next_sn_mutex);
    return next_sn_meta.entry_sn;
}

LogSequenceMetadata Loglet::nextLogSequenceMetadata() const noexcept
{
    std::scoped_lock lock(next_sn_mutex);
    return next_sn_meta;
}

size_t Loglet::size() const noexcept
{
    size_t total_bytes = 0;
    segments->apply([&](LogSegmentPtr & segment) {
        total_bytes += segment->size();
        return false;
    });
    return total_bytes;
}

std::optional<FileLogEntries::LogSequencePosition> Loglet::translateSequence(const LogSegmentPtr & segment, int64_t sn) const
{
    auto res = sn_pos_index->leftLowerBoundPositionForSequence(sn);
    if (res.hasError() || !res.result)
        return segment->translateSequence(sn, /*start_position=*/0);

    auto [segment_id, file_position] = segmentIDAndFilePosition(res.result->file_position);
    if (segment_id == segment->getID())
    {
        if (res.result->sn == sn)
            return FileLogEntries::LogSequencePosition{sn, file_position, 0};

        return segment->translateSequence(sn, /*start_position=*/file_position);
    }
    else
    {
        /// sn_pos_index may be got truncated first and system rebooted, it doesn't have the last log segment's index
        /// but the previous segment
        LOG_INFO(
            logger,
            "Sequence position from sn_pos index has segment_id={} sn={} file_position={}, and current segment has segment_id={} "
            "base_sn={}. Search from beginning of the segment.",
            segment_id,
            res.result->sn,
            file_position,
            segment->getID(),
            segment->baseSequence());

        return segment->translateSequence(sn, /*start_position=*/0);
    }
}

/// [low_sn, high_sn)
CallResult<std::vector<EpochSequence>> Loglet::epochSequences(int64_t low_sn, int64_t high_sn) const
{
    assert(low_sn < high_sn);

    auto segment = segments->floorSegment(low_sn);
    if (!segment)
        return CallResult<std::vector<EpochSequence>>{DB::ErrorCodes::SEQUENCE_OUT_OF_RANGE};

    uint64_t file_pos = 0;
    if (low_sn != segment->baseSequence())
    {
        auto translated_lsn = translateSequence(segment, low_sn);
        if (!translated_lsn)
            return CallResult<std::vector<EpochSequence>>{DB::ErrorCodes::RECORD_NOT_FOUND};

        assert(translated_lsn->sn == low_sn);
        file_pos = translated_lsn->file_position;
    }

    Stopwatch stopwatch;

    LOG_INFO(logger, "Fetching epoch sequence for range=[{}, {})", low_sn, high_sn);

    CallResult<std::vector<EpochSequence>> result;
    result.result.reserve(high_sn - low_sn);

    while (true)
    {
        auto file_log_entries = segment->fetch(file_pos, size() - file_pos);
        file_log_entries->applyLogEntryMetadata(
            [&](cluster::EntryPtr meta_entry, uint64_t) {
                if (meta_entry->sn < high_sn)
                    result.result.emplace_back(1, meta_entry->sn); // epoch

                return meta_entry->sn + 1 < high_sn;
            },
            {});

        if (result.result.empty())
            return CallResult<std::vector<EpochSequence>>{DB::ErrorCodes::RECORD_NOT_FOUND};

        if (result.result.back().sn + 1 >= high_sn)
            break;

        /// Move to next segment
        LOG_INFO(
            logger, "Moving to next segment to fetch epoch sequence={}, expected range=[{}, {})", result.result.back().sn, low_sn, high_sn);

        segment = segments->floorSegment(result.result.back().sn + 1);
        file_pos = 0;
    }

    assert(result.result.front().sn == low_sn);
    assert(result.result.back().sn + 1 == high_sn);

    LOG_INFO(logger, "Enn of fetching epoch sequence for range=[{}, {}), took={}ms", low_sn, high_sn, stopwatch.elapsedMilliseconds());

    return result;
}
}
