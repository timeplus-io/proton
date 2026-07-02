#include <Cluster/Common/Constants.h>
#include <Cluster/Common/StreamShard.h>
#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>
#include <Cluster/LocalLog/Indexes/EpochSequenceIndex.h>
#include <Cluster/LocalLog/Indexes/SequencePositionIndex.h>
#include <Cluster/LocalLog/Indexes/TimestampSequenceIndex.h>
#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/LocalLog/Log/LogLoader.h>
#include <Cluster/LocalLog/Log/Loglet.h>

#include <base/ClockUtils.h>
#include <Common/ErrorCodes.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int FAILED_TO_ROLL_LOG_SEGMENT;
extern const int TOO_LARGE_RECORD;
extern const int SEQUENCE_COMPACTED_AWAY;
extern const int SEQUENCE_UNAVAILABLE;
extern const int INVALID_SEQUENCE;
extern const int OK;
extern const int STREAM_PAUSED;
}
}

namespace cluster::nlog
{

LogPtr Log::create(
    const StreamShard & stream_shard,
    const fs::path & log_dir,
    LogConfigPtr log_config,
    bool had_clean_shutdown,
    DB::NLOG::BackgroundSchedulePool & scheduler,
    ThreadPool & adhoc_scheduler)
{
    /// Create the log directory if it doesn't exist
    fs::create_directories(log_dir);

    auto logger = getLogger(stream_shard.logName());

    auto epoch_sn_index = std::make_unique<EpochSequenceIndex>(
        log_dir, log_config->leader_epoch_index_log_size, log_config->leader_epoch_index_log_preallocate, logger);

    auto event_ts_sn_index = std::make_unique<TimestampSequenceIndex>(
        log_dir, log_config->timestamp_index_log_size, log_config->timestamp_index_log_preallocate, logger);

    auto sn_pos_index = std::make_unique<SequencePositionIndex>(
        log_dir, log_config->log_position_index_log_size, log_config->log_position_index_log_preallocate, logger);

    auto segments = std::make_shared<LogSegments>();

    auto next_sn_meta
        = LogLoader::load(log_dir, log_config, had_clean_shutdown, epoch_sn_index, event_ts_sn_index, sn_pos_index, segments, logger);
    chassert(next_sn_meta.entry_sn >= Constants::LogStartSN);
    chassert(!segments->empty());

    auto loglet = std::make_unique<Loglet>(
        stream_shard,
        log_dir,
        log_config,
        next_sn_meta,
        segments,
        std::move(event_ts_sn_index),
        std::move(sn_pos_index),
        scheduler,
        adhoc_scheduler,
        logger);

    return std::make_shared<Log>(
        segments->firstSegment()->baseSequence(), std::move(epoch_sn_index), std::move(loglet), getLogger(stream_shard.logName()));
}

Log::Log(int64_t log_start_sn_, EpochSequenceIndexPtr leader_epoch_index_, LogletPtr loglet_, LoggerPtr logger_)
    : log_config(loglet_->config())
    , log_start_sn(log_start_sn_)
    , epoch_sn_index(std::move(leader_epoch_index_))
    , loglet(std::move(loglet_))
    , committed_sn(Constants::LogStartSN - 1) /// committed sn may be corrected a bit later
    , cached(log_config->max_cached_entries_per_shard)
    , logger(logger_)
{
    chassert(log_start_sn >= Constants::LogStartSN);
    LOG_INFO(
        logger,
        "Log constructor: log_start_sn={}, initial next_sn={}, log_ptr={}",
        log_start_sn.load(),
        loglet->nextLogSequence(),
        static_cast<const void *>(this));
}

Log::~Log()
{
    shutdown();
    applied_sn_getter = {};
}

void Log::shutdown()
{
    if (closed.test_and_set())
        return;

    /// notify readers who waits on new data
    log_cv.notify_all();
}

void Log::restart()
{
    closed.clear();
}

CallResult<size_t> Log::append(const cluster::EntryPtrs & entries)
{
    chassert(!entries.empty());

    if (isInmemory())
        return appendInMemory(entries);

    chassert(nextLogSequence() == entries.front()->sn);

    CallResult<size_t> result;
    size_t entry_index = 0;
    size_t total_entries = entries.size();
    while (true)
    {
        if (entry_index + IOV_MAX >= total_entries)
        {
            auto batch_result = append(std::span(entries.begin() + entry_index, total_entries - entry_index));
            result.result += batch_result.result;
            result.error_code = batch_result.error_code;
            break;
        }
        else
        {
            if (auto batch_result = append(std::span(entries.begin() + entry_index, IOV_MAX)); !batch_result.hasError())
            {
                result.result += batch_result.result;
            }
            else
            {
                result.result += batch_result.result;
                result.error_code = batch_result.error_code;
                break;
            }

            entry_index += IOV_MAX;
        }
    }

    return result;
}

CallResult<size_t> Log::append(std::span<const cluster::EntryPtr> batch)
{
    chassert(!batch.empty() && batch.size() <= IOV_MAX);
    LOG_INFO(
        logger,
        "Log::append(batch): Appending batch of {} entries, expected sn range [{}, {}]",
        batch.size(),
        batch.front()->sn,
        batch.back()->sn);
    chassert(nextLogSequence() == batch.front()->sn);

    std::vector<ByteVector> batch_data;
    batch_data.reserve(batch.size());

    TimestampSequence max_event_ts;

    CallResult<size_t> result;
    uint32_t bytes = 0;
    {
        for (const auto & entry : batch)
        {
            if (!entry->dataEntry() || entry->dataEmpty())
                LOG_INFO(logger, "Appending control or empty data entry={} with sn={}", entry->typeString(), entry->sn);

            /// Commit epoch sn mapping first if necessary
            /// We will need to hold index mutex since when we retention log segments,
            /// we may trimHead leader epoch index in a different thread.
            /// trim leader epoch index at tail has no race with append since they happen in single Raft thread.
            if (auto res = epoch_sn_index->maybeIndexEpochStartSequence(EpochSequence{1, entry->sn}); res.hasError())
            {
                LOG_ERROR(logger, "Failed to index sn={} mapping, error={}", entry->sn, res.errorString());
                result.error_code = res.error_code;
                return result;
            }

            if (entry->max_event_timestamp > max_event_ts.timestamp)
            {
                max_event_ts.timestamp = entry->max_event_timestamp;
                max_event_ts.sn = entry->sn;
            }

            ///  if (entry->append_timestamp > max_timestamps.append_ts.timestamp)
            ///  {
            ///      max_timestamps.append_ts.timestamp = entry->append_timestamp;
            ///      max_timestamps.append_ts.sn = entry->sn;
            ///  }

            batch_data.push_back(entry->serialize(/*version_=*/1));
            bytes += batch_data.back().size();
        }

        {
            auto res = maybeRoll(bytes, batch.front()->sn);
            if (res.hasError())
            {
                result.error_code = DB::ErrorCodes::FAILED_TO_ROLL_LOG_SEGMENT;
                return result;
            }

            auto start_file_position = res.result->size();

            if (result = loglet->append(batch, batch_data, bytes, max_event_ts); result.hasError())
                return result;

            cached.append(batch, batch_data, res.result->baseSequence(), start_file_position);
        }
    }

    return result;
}

CallResult<int64_t> Log::appendLocal(cluster::EntryPtr entry)
{
    if (isPaused())
    {
        LOG_DEBUG(logger, "Append rejected: log is paused for stream {}", streamShard().string());
        return {0, DB::ErrorCodes::STREAM_PAUSED};
    }

    // No epoch validation needed
    chassert(entry->sn <= 0);

    if (entry->payloadSize() > maxEntrySize())
        return {0, DB::ErrorCodes::TOO_LARGE_RECORD};

    /// For local append, hold the lock and insert entry to the log
    /// since writers are in-parallel appending which is different than distributed cluster.
    {
        std::unique_lock lock{log_mutex};

        /// Update the sequence number and append time for this entry,
        /// only for local append
        auto next_sn = nextLogSequence();
        LOG_TRACE(
            logger,
            "appendLocal: Before assignment - next_sn={}, log_start_sn={}, log_ptr={}",
            next_sn,
            log_start_sn.load(),
            static_cast<const void *>(this));
        entry->sn = next_sn;
        entry->append_timestamp = DB::UTCMilliseconds::now();

        ByteVector data{entry->serialize(/*version_=*/1)};

        chassert(nextLogSequence() == entry->sn);

        auto res = maybeRoll(static_cast<uint32_t>(data.size()), entry->sn);
        if (res.hasError())
            return {0, DB::ErrorCodes::FAILED_TO_ROLL_LOG_SEGMENT};

        LogSequenceMetadata current_metadata{1, entry->sn, res.result->baseSequence(), res.result->size()};

        /// Append the log entries, and increment the loglet end sn immediately after the append
        if (isInmemory())
        {
            /// Update the sequence number manually here
            loglet->updateNextLogSequence(entry->sn + 1, 0, 0);
        }
        else
        {
            if (auto result = loglet->append(data, entry); result.hasError())
                return {0, result.error_code};
        }

        /// We will need hold the log_mutex lock to update the cache since cache is a deque
        /// which requires sequence order
        cached.append(entry, current_metadata, loglet->nextLogSequenceMetadata().toFetchHint());
    }

    if (loglet->needFlush())
        flush();

    advanceCommittedSequence(1, entry->sn);

    return {entry->sn, DB::ErrorCodes::OK};
}

CallResult<size_t> Log::appendInMemory(const cluster::EntryPtrs & entries)
{
    chassert(isInmemory());

    for (const auto & entry : entries)
    {
        // No epoch validation needed

        /// We will need hold the log_mutex lock to update the cache since cache is a deque
        /// which requires sequence order
        /// Update the sequence number manually here
        LogSequenceMetadata current_metadata{1, entry->sn};
        loglet->updateNextLogSequence(entry->sn + 1, 0, 0);
        cached.append(entry, current_metadata, loglet->nextLogSequenceMetadata().toFetchHint());
    }

    const auto & entry = entries.back();
    advanceCommittedSequence(1, entry->sn);

    return {};
}

/// Roll the log over to a new empty log segment if necessary
/// \return The currently active segment after (perhaps) rolling to a new segment when success or don't need roll.
///         return nullptr and error code if failed to roll a new segment.
CallResult<LogSegmentPtr> Log::maybeRoll(uint32_t append_size, int64_t next_sn)
{
    auto segment = loglet->activeSegment();
    if (!segment->shouldRoll(log_config, append_size))
        return CallResult{std::move(segment)};

    LOG_INFO(
        logger,
        "Rolling new log segment base_sn={} size={} size_threshold={} next_sn={}",
        segment->baseSequence(),
        segment->size(),
        log_config->segment_size,
        next_sn);

    Stopwatch stopwatch;

    auto res = loglet->roll(next_sn);
    if (res.hasError())
        /// Failed to roll
        return res;

    LOG_INFO(
        logger,
        "Rolling new log segment base_sn={} size={} size_threshold={} next_sn={}, took {}ms",
        segment->baseSequence(),
        segment->size(),
        log_config->segment_size,
        next_sn,
        stopwatch.elapsedMilliseconds());

    return res;
}

LogSequenceMetadata Log::nextSequenceMetadata(FetchIsolation isolation, bool with_committed_lock_held) const
{
    switch (isolation)
    {
        case FetchIsolation::Committed:
            if (with_committed_lock_held)
                return LogSequenceMetadata{1 /*epoch*/, committed_sn + 1, logActiveSegmentBaseSequence()}; /// epoch=1
            else
                return LogSequenceMetadata{1 /*epoch*/, committedSequence() + 1, logActiveSegmentBaseSequence()}; /// epoch=1
        case FetchIsolation::LogEnd:
            /// Log end sn is basically dirty
            return loglet->nextLogSequenceMetadata();
    }
}

/// \param max_wait_ms if it is > zero, block wait inline
LogSequenceMetadata Log::nextFetchSequenceOrWait(int64_t & sn, int64_t max_wait_ms, FetchIsolation isolation) const
{
    auto next_sn_metadata = nextSequenceMetadata(isolation);

    if (sn == Constants::EarliestSN)
        sn = log_start_sn;
    else if (sn == Constants::LatestSN)
        sn = next_sn_metadata.entry_sn;
    else if (sn < Constants::LogStartSN)
        sn = Constants::LogStartSN;

    chassert(sn >= Constants::LogStartSN);

    if (sn < next_sn_metadata.entry_sn || max_wait_ms <= 0)
        /// We have the data already or we don't want wait, return directly
        return next_sn_metadata;

    auto next_log_sn_changed = [this, &next_sn_metadata, isolation] {
        auto new_next_sn_metadata = nextSequenceMetadata(isolation, true);

        bool sn_changed = new_next_sn_metadata.entry_sn > next_sn_metadata.entry_sn;
        if (sn_changed)
        {
            /// next sequence metadata has changed, we can use the latest one to read more data
            next_sn_metadata = new_next_sn_metadata;

            return true;
        }

        if (closed.test())
            return true;

        return false;
    };

    std::unique_lock lock{log_mutex};
    log_cv.wait_for(lock, std::chrono::milliseconds(max_wait_ms), next_log_sn_changed);

    return next_sn_metadata;
}

CallResult<FetchResult>
Log::fetch(int64_t sn, uint64_t max_size, int64_t max_wait_ms, std::optional<FetchHint> fetch_hint, FetchIsolation isolation) const
{
    auto start_sn = logStartSequence();
    if (start_sn > sn && sn >= Constants::LogStartSN)
    {
        auto log_committed_sn = committedSequence();
        LOG_ERROR(
            logger, "Fetching sn={} has been compacted away, current log start_sn={} committed_sn={}", sn, start_sn, log_committed_sn);

        /// Log entries were compacted away
        return CallResult<FetchResult>(
            {.log_start_sn = start_sn,
             .log_committed_sn = committed_sn,
             .fetch_hint = FetchHint{.sn = start_sn, .segment_base_sn = logFirstSegmentBaseSequence()}},
            DB::ErrorCodes::SEQUENCE_COMPACTED_AWAY);
    }

    auto next_sn_metadata{nextFetchSequenceOrWait(sn, max_wait_ms, isolation)};

    // auto current_committed_sn = committedSequence();
    // LOG_INFO(logger, "fetch: sn={} next_sn_metadata.entry_sn={} committed_sn={} isolation={}",
    //          sn, next_sn_metadata.entry_sn, current_committed_sn, static_cast<int>(isolation));

    if (next_sn_metadata.entry_sn <= sn)
    {
        /// No more data to fetch
        if (next_sn_metadata.entry_sn != sn || !fetch_hint || fetch_hint->empty())
            /// Clean the fetch hint only when there is a mismatch for sn or there is no fetch hint
            return CallResult<FetchResult>(
                {.log_start_sn = start_sn, .log_committed_sn = committedSequence(), .fetch_hint = next_sn_metadata.toFetchHint()});
        else
            /// Otherwise stick to the original fetch hint
            return CallResult<FetchResult>(
                {.log_start_sn = start_sn, .log_committed_sn = committedSequence(), .fetch_hint = fetch_hint.value()});
    }

    {
        auto [fetch_result, try_fs] = cached.fetch(sn, next_sn_metadata.entry_sn, max_size, loglet->isInmemory());
        if (!fetch_result.empty() || !try_fs)
        {
            fetch_result.log_start_sn = start_sn;
            fetch_result.log_committed_sn = committedSequence();
            return CallResult{fetch_result};
        }
    }

    /// LOG_INFO(logger, "fetch at sn={} max_sn_meta={}", sn, max_sn_metadata.string());
    /// When we reach here, we either timed out or there are new records appended
    auto fetch_result = loglet->fetch(sn, max_size, next_sn_metadata, fetch_hint);
    fetch_result.result.log_start_sn = start_sn;
    fetch_result.result.log_committed_sn = committedSequence();
    return fetch_result;
}

/// Called from backfill replication, since it doesn't care about the log start / committed sn, they are not setup
/// in the fetch result
CallResult<FetchResult>
Log::fetchRange(int64_t low_sn, int64_t high_sn, uint64_t max_size, std::optional<FetchHint> fetch_hint, FetchIsolation isolation) const
{
    chassert(low_sn < high_sn);

    auto start_sn = logStartSequence();
    if (start_sn > low_sn)
        /// Log entries were compacted away
        return CallResult<FetchResult>(
            {.log_start_sn = start_sn,
             .log_committed_sn = committedSequence(),
             .fetch_hint = FetchHint{.sn = start_sn, .segment_base_sn = logFirstSegmentBaseSequence()}},
            DB::ErrorCodes::SEQUENCE_COMPACTED_AWAY);

    auto next_sn_metadata{nextFetchSequenceOrWait(low_sn, /*max_wait_ms=*/0, isolation)};
    if (next_sn_metadata.entry_sn >= high_sn)
        next_sn_metadata.entry_sn = high_sn;
    else
        /// There are 2 cases. For these 2 cases, we both return empty entries since the current log
        /// doesn't cover the full range of [low_sn, high_sn)
        /// next_sn_metadata.entry_sn < low_sn means there are no entries in [low_sn, high_sn) range or
        /// next_sn_metadata.entry_sn < high_sn means there are partial entries in range [low_sn, high_sn)
        return CallResult<FetchResult>({.fetch_hint = FetchHint{.sn = next_sn_metadata.entry_sn}}, DB::ErrorCodes::SEQUENCE_UNAVAILABLE);

    auto [fetch_result, try_fs] = cached.fetch(low_sn, next_sn_metadata.entry_sn, max_size, loglet->isInmemory());
    if (!fetch_result.empty() || !try_fs)
        return CallResult{fetch_result};

    /// LOG_INFO(logger, "fetch at sn={} max_sn_meta={}", sn, max_sn_metadata.string());

    /// When we reach here, we either timed out or there are records on disk but not in cache.
    return loglet->fetch(low_sn, max_size, next_sn_metadata, fetch_hint);
}

/// \trim truncates log entries at the tail of the log: truncate entries in range [target_sn, ...)
/// It is only invoked in single Raft thread. We need pay attention to the race with Fetch threads.
int Log::trim(int64_t target_sn)
{
    if (target_sn < Constants::LogStartSN)
    {
        LOG_ERROR(logger, "Invalid sn={} to trim", target_sn);
        return DB::ErrorCodes::INVALID_SEQUENCE;
    }

    /// Committed sn shall never be truncated, allow trimming up to committed point
    /// There are 2 cases we will violate this, 1) unit testing, 2) corrupted committed sn which needs to be truncated
    /// via system command
    /// chassert(target_sn > committed_sn);

    auto log_end_sn = loglet->nextLogSequence();
    if (target_sn < log_end_sn)
    {
        LOG_INFO(logger, "Truncating sn range [{}, {}), committed_sn={}", target_sn, log_end_sn, committed_sn.load());

        if (loglet->firstSegment()->baseSequence() > target_sn)
        {
            /// Optimized case
            if (auto err = trimFullyAndStartAt(target_sn, {}); err != DB::ErrorCodes::OK)
                return err;
        }
        else
        {
            if (auto err = loglet->trim(target_sn); err != DB::ErrorCodes::OK)
                return err;

            auto next_log_sn_meta_cached = cached.trim(target_sn);
            if (next_log_sn_meta_cached)
            {
                chassert(*next_log_sn_meta_cached == loglet->nextLogSequenceMetadata().toFetchHint());
                LOG_INFO(
                    logger, "Truncated tail cache at sn={}, tail cache has next_log_sn={} now", target_sn, next_log_sn_meta_cached->sn);
            }
            else
            {
                auto last_sn = cached.lastLogSequence();
                LOG_WARNING(logger, "Haven't truncated tail cache, tail sn={}", last_sn ? last_sn.value() : 0);
            }

            if (auto err = epoch_sn_index->trim(target_sn); err != DB::ErrorCodes::OK)
                return err;

            log_start_sn = std::min(target_sn, log_start_sn.load());
            if (target_sn < committed_sn)
                committed_sn = target_sn;
        }
        return DB::ErrorCodes::OK;
    }
    else if (target_sn == log_end_sn)
    {
        /// We like to trim the epoch index since we commit leader epoch index first and then append the log.
        /// So if the node was a leader and inserted the first start log entry which doesn't have data (empty
        /// log entry), but failed to append any data entries before another leader was elected.
        return epoch_sn_index->trim(target_sn);
    }
    else
    {
        LOG_INFO(logger, "Truncating to {} has no effect as the largest sn in the log is {}", target_sn, log_end_sn);
        return DB::ErrorCodes::OK;
    }
}

/// \param prev_epoch_sn if it is not nullopt, it is the prev epoch sn before the new_sn
/// Delete all data in the log and start at the new offset and commit the prev_epoch_sn
/// if it is not nullopt to leader epoch index
int Log::trimFullyAndStartAt(int64_t new_sn, std::optional<EpochSequence> prev_epoch_sn)
{
    if (auto err = loglet->trimFullyAndStartAt(new_sn); err != DB::ErrorCodes::OK)
        return err;

    cached.clear();

    /// It is fine to ignore that if failed to trim / remove the epoch index
    if (prev_epoch_sn)
    {
        chassert(prev_epoch_sn->sn + 1 == new_sn);
        epoch_sn_index->trimHead(new_sn);
        epoch_sn_index->maybeIndexEpochStartSequence(*prev_epoch_sn, /*sync=*/true);
    }
    else
    {
        epoch_sn_index->remove();
    }

    log_start_sn = new_sn;
    committed_sn = new_sn - 1;

    return DB::ErrorCodes::OK;
}

int64_t Log::advanceCommittedSequence(uint64_t new_epoch, int64_t new_committed_sn)
{
    uint64_t prev_epoch = committed_epoch;
    int64_t prev_commit_sn = committed_sn;

    LOG_TRACE(
        logger,
        "advanceCommittedSequence: new_epoch={} new_committed_sn={} prev_epoch={} prev_commit_sn={}",
        new_epoch,
        new_committed_sn,
        prev_epoch,
        prev_commit_sn);

    if (prev_epoch > new_epoch)
        return prev_commit_sn;

    auto next_log_sn = nextLogSequence();
    if (new_committed_sn >= next_log_sn)
    {
        LOG_ERROR(
            logger,
            "New new_committed_sn={} to be committed in epoch={} exceeds current next_log_sn={} current committed_epoch={} current "
            "committed_sn={}",
            new_committed_sn,
            new_epoch,
            next_log_sn,
            prev_epoch,
            prev_commit_sn);

        return committed_sn;
    }

    if (new_committed_sn > prev_commit_sn)
    {
        chassert(new_committed_sn >= committed_sn);
        committed_sn = new_committed_sn;

        chassert(new_epoch >= prev_epoch);
        chassert(new_epoch >= committed_epoch);
        committed_epoch = new_epoch;

        LOG_TRACE(logger, "advanceCommittedSequence: Updated committed_sn to {}", committed_sn.load());
    }

    log_cv.notify_all();

    return prev_commit_sn;
}

bool Log::maybeIncrementLogStartSequence(int64_t new_log_start_sn, std::string_view reason)
{
    auto current_start_sn = logStartSequence();
    if (new_log_start_sn > current_start_sn)
    {
        LOG_INFO(logger, "Progress log start sequence from {} to {} due to {}", current_start_sn, new_log_start_sn, reason);
        updateLogStartSequence(new_log_start_sn);
        return true;
    }
    return false;
}

int64_t Log::sequenceForTimestamp(int64_t ts, bool append_time) const
{
    /// Segment deletion and inverted indexes can be cleaned up in an async way
    /// So it is possible we have a staled lookup sn whose containing segment is already deleted
    auto res = loglet->sequenceForTimestamp(ts, append_time);
    if (!res.hasError() && res.result && res.result.value() >= log_start_sn)
        return res.result.value();

    return log_start_sn;
}

size_t Log::deleteOldSegments(int64_t applied_sn)
{
    LOG_DEBUG(logger, "Garbage collecting applied_sn={} {}", applied_sn, log_config->string());
    return deleteLogStartSequenceBreachedSegments(applied_sn) + deleteRetentionSizeBreachedSegments(applied_sn)
        + deleteRetentionTimeBreachedSegments(applied_sn);
}

size_t Log::deleteLogStartSequenceBreachedSegments(int64_t applied_sn)
{
    auto should_delete
        = [start_sn = logStartSequence()](LogSegmentPtr, LogSegmentPtr next_segment) { return next_segment->baseSequence() <= start_sn; };

    return deleteOldSegments(should_delete, applied_sn, "start_sequence_breached");
}

size_t Log::deleteRetentionSizeBreachedSegments(int64_t applied_sn)
{
    auto retention_size = log_config->retention_size;
    if (retention_size == 0)
        return 0;

    auto total_bytes = loglet->size();
    if (total_bytes < retention_size)
        return 0;

    if (total_bytes < log_config->min_size_to_keep)
        return 0;

    auto should_delete = [total_bytes, retention_size, min_size_to_keep = log_config->min_size_to_keep](
                             LogSegmentPtr prev_segment, [[maybe_unused]] LogSegmentPtr current_segment) mutable {
        chassert(prev_segment->baseSequence() < current_segment->baseSequence());

        auto prev_seg_size = prev_segment->size();
        if (total_bytes > retention_size && total_bytes > prev_seg_size && (total_bytes - prev_seg_size) > min_size_to_keep)
        {
            total_bytes -= prev_seg_size;
            return true;
        }
        return false;
    };

    return deleteOldSegments(should_delete, applied_sn, "retention_size_breached");
}

size_t Log::deleteRetentionTimeBreachedSegments(int64_t applied_sn)
{
    auto retention_ms = log_config->retention_ms;
    if (retention_ms <= 0)
        return 0;

    auto total_bytes = loglet->size();
    if (total_bytes < log_config->min_size_to_keep)
        return 0;

    auto start_ms = DB::localNowMilliseconds();
    auto should_delete = [start_ms, retention_ms, total_bytes, min_size_to_keep = log_config->min_size_to_keep](
                             LogSegmentPtr segment, LogSegmentPtr) mutable {
        if (auto res = segment->lastModified(); !res.hasError())
        {
            if (start_ms - res.result > retention_ms && total_bytes > segment->size() && total_bytes > min_size_to_keep)
            {
                total_bytes -= segment->size();
                return true;
            }
            return false;
        }

        return false;
    };

    return deleteOldSegments(should_delete, applied_sn, "retention_time_breached");
}

size_t Log::deleteOldSegments(std::function<bool(LogSegmentPtr, LogSegmentPtr)> should_delete, int64_t applied_sn, std::string_view reason)
{
    auto deletable = loglet->deletableSegments(should_delete, applied_sn);
    if (deletable.empty())
        return 0;

    LOG_DEBUG(
        logger,
        "Found {} segments to delete, applied_sn={} retention_ms={} retention_size={} min_size_to_keep={} reason={}",
        deletable.size(),
        applied_sn,
        log_config->retention_ms,
        log_config->retention_size,
        log_config->min_size_to_keep,
        reason);

    /// FIXME, more granularity control by rolling current segment
    /// Note background segment retention for now don't have conflicts with tail cache in practice since
    /// we always keep the active segment around and tail cache is assume to have only contain records
    /// in active segment

    if (auto err = loglet->removeSegments(deletable, reason, {}); err != DB::ErrorCodes::OK)
        LOG_ERROR(logger, "Failed to remove log segments for retention, error={}", DB::ErrorCodes::getName(err));

    /// After loglet->removeSegments, the in-memory segments is already changed to up to date
    /// although the actual log deletion can be scheduled in background
    /// Increment log start sequence
    maybeIncrementLogStartSequence(firstSegment()->baseSequence(), reason);

    chassert(logStartSequence() < applied_sn);

    return deletable.size();
}

std::optional<uint64_t> Log::latestLeaderEpoch() const
{
    auto res = epoch_sn_index->latestLeaderEpochSequence();
    if (!res.hasError() && res.result)
    {
        /// Since we commit leader epoch index first and then commit the append,
        /// it is possible a larger epoch / sn in index, which is larger than
        /// the epoch / sn of the log end. If this is the case, resolve the
        /// latest indexed epoch for the last durable log sequence.
        auto next_sn = nextLogSequence();
        if (next_sn > res.result->sn)
            return res.result->epoch;

        res = epoch_sn_index->leaderEpochSequenceFor(next_sn - 1);
        if (!res.hasError() && res.result)
        {
            chassert(res.result->sn < next_sn);
            return res.result->epoch;
        }
    }

    return {};
}

std::optional<uint64_t> Log::leaderEpochFor(int64_t sn) const
{
    chassert(sn >= 0);

    if (sn < Constants::LogStartSN)
        return Constants::LogStartEpoch;

    auto next_sn = nextLogSequence();
    chassert(sn < next_sn);

    /// Last log sequence number
    if (sn == next_sn - 1)
        return latestLeaderEpoch();

    auto res = epoch_sn_index->leaderEpochSequenceFor(sn);
    if (!res.hasError() && res.result)
        return res.result->epoch;

    /// Slowest path
    LOG_WARNING(logger, "Fallback to reading log to get the epoch for sn={}", sn);

    /// We don't know the epoch for sequence sn if we garbage collect
    /// it from the epoch index, then read it from log
    // Always return epoch 1
    auto fetch_result = fetch(sn, /*max_size=*/1ull, /*max_wait_ms=*/0ll, /*fetch_hint=*/{}, FetchIsolation::Committed);
    if (!fetch_result.result.entries.empty())
    {
        chassert(fetch_result.result.entries.front()->sn == sn);
        return 1; // epoch
    }

    /// We don't know epoch for this sn (probably truncated from index and the log)
    return {};
}

/// [..., last_committed_sn)
CallResult<CommittedSequences> Log::committedSequences(int64_t last_committed_sn, uint64_t max_entries) const
{
    auto local_committed = committedSequence();
    int64_t local_log_start_sn = logStartSequence();
    int64_t next_sn = nextLogSequence();

    last_committed_sn = last_committed_sn > Constants::LogStartSN ? std::min(last_committed_sn, local_committed) : local_committed;

    int64_t start_committed_sn = local_log_start_sn;

    if (local_log_start_sn + static_cast<int64_t>(max_entries) < last_committed_sn)
        start_committed_sn = last_committed_sn - static_cast<int64_t>(max_entries);

    if (start_committed_sn + 1 >= last_committed_sn)
        return {};

    chassert(start_committed_sn < last_committed_sn);

    auto sequences = cached.epochSequences(start_committed_sn, last_committed_sn);
    if (!sequences.empty())
        return CallResult<CommittedSequences>(CommittedSequences{.epoch_sns = std::move(sequences), .log_next_sn = next_sn});

    auto result = loglet->epochSequences(start_committed_sn, last_committed_sn);
    if (!result.hasError())
        return CallResult<CommittedSequences>(CommittedSequences{.epoch_sns = std::move(result.result), .log_next_sn = next_sn});
    else
        return CallResult<CommittedSequences>{result.error_code};
}

int Log::remove()
{
    if (epoch_sn_index)
        epoch_sn_index->close();

    return loglet->remove();
}

void Log::pause()
{
    bool expected = false;
    if (paused.compare_exchange_strong(expected, true))
        LOG_INFO(logger, "Paused log for stream {}", streamShard().string());
    else
        LOG_DEBUG(logger, "Log for stream {} is already paused", streamShard().string());
}

void Log::resume()
{
    bool expected = true;
    if (paused.compare_exchange_strong(expected, false))
        LOG_INFO(logger, "Resumed log for stream {}", streamShard().string());
    else
        LOG_DEBUG(logger, "Log for stream {} is already resumed", streamShard().string());
}

bool Log::isPaused() const
{
    return paused.load(std::memory_order_acquire);
}

void Log::recover()
{
    /// Reset to last known good state
    paused.store(false, std::memory_order_release);

    /// For recovery, we just clear the paused state
    /// The actual log recovery (if needed) would happen at a higher level
    LOG_INFO(logger, "Recovered log for stream {}", streamShard().string());
}

}
