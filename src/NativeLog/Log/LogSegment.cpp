#include "LogSegment.h"
#include "Loglet.h"

#include <NativeLog/Common/LogAppendDescription.h>
#include <Common/logger_useful.h>

namespace nlog
{
LogSegment::LogSegment(
    const fs::path & log_dir,
    int64_t base_sn_,
    const LogConfig & log_config,
    Poco::Logger * logger_,
    bool file_already_exists,
    bool /*preallocate*/,
    const std::string & file_suffix)
    : base_sn(base_sn_)
    , max_index_size(log_config.max_index_size)
    , index_interval_bytes(static_cast<int32_t>(log_config.index_interval_bytes))
    , log(FileRecords::open(Loglet::logFile(log_dir, base_sn_, file_suffix), file_already_exists, false))
    , max_etimestamp_and_sn_so_far(-1, -1)
    , max_atimestamp_and_sn_so_far(-1, -1)
    , indexes(Loglet::indexFileDir(log_dir, base_sn, file_suffix), base_sn_, logger_)
    , logger(logger_)
{
    max_etimestamp_and_sn_so_far = indexes.lastIndexedEventTimeSequence();
    max_atimestamp_and_sn_so_far = indexes.lastIndexedAppendTimeSequence();
}

int64_t LogSegment::append(const ByteVector & record, const LogAppendDescription & append_info)
{
    auto physical_position = log->size();
    if (physical_position == 0)
        rolling_based_timestamp = append_info.max_event_timestamp;

//    LOG_TRACE(
//        logger,
//        "Appending {} bytes at sn={} at file position {} with max event timestamp {}",
//        record.size(),
//        append_info.seq_metadata.record_sn,
//        physical_position,
//        append_info.max_event_timestamp);

    /// ensureSNInRange(largest_sn);

    /// Append the messages
    auto n = log->append(record);

    LOG_TRACE(
        logger,
        "Appended {} bytes at sn={} at file_position={} with max_event_timestamp={}",
        n,
        append_info.seq_metadata.record_sn,
        physical_position,
        append_info.max_event_timestamp);

    /// Update the in memory max timestamp and corresponding sn.
    if (append_info.max_event_timestamp > max_etimestamp_and_sn_so_far.key)
        max_etimestamp_and_sn_so_far = TimestampSequence(append_info.max_event_timestamp, append_info.seq_metadata.record_sn);

    if (append_info.append_timestamp > max_atimestamp_and_sn_so_far.key)
        max_atimestamp_and_sn_so_far = TimestampSequence(append_info.append_timestamp, append_info.seq_metadata.record_sn);

    if (bytes_since_last_index_entry > index_interval_bytes)
    {
        indexes.index(append_info.seq_metadata.record_sn, physical_position, max_etimestamp_and_sn_so_far, max_atimestamp_and_sn_so_far);
        bytes_since_last_index_entry = 0;
    }

    bytes_since_last_index_entry += record.size();
    return physical_position;
}

FetchDataDescription LogSegment::read(int64_t start_sn, uint64_t max_size, uint64_t max_position, std::optional<uint64_t> position)
{
    FileRecords::LogSequencePosition lsn{start_sn, 0, 1};
    if (!position || log->size() < *position)
    {
        /// If position is invalid or beyond of the log size,
        /// we will need translate the start_sn to a physical file position;
        /// otherwise we can save this translation if client pass in a file position (we trust this is a good file position)
        lsn = translateSequence(start_sn);
        if (!lsn.isValid())
            return {};
    }
    else
        /// Client passed a file position which is in range, use it. This is an optimization which saves a file position translation
        lsn.position = *position;

    assert(lsn.position <= max_position);

    LogSequenceMetadata sn_metadata(start_sn, baseSequence(), lsn.position);

    auto adjusted_max_size = std::max(max_size, lsn.size);
    if (adjusted_max_size == 0)
        return {sn_metadata, {}};

    auto max_to_fetch = max_position - lsn.position;
    if (adjusted_max_size >= max_to_fetch)
    {
        /// We will read beyond or at the max_position. If reading the single record at offset start_sn will exceed max_position
        /// then we will need wait util bigger max_position
        if (lsn.size > max_to_fetch)
            return {sn_metadata, {}};

        return {sn_metadata, log->slice(lsn.position, max_to_fetch)};
    }
    else
    {
        /// Within the max position
        /// We like to slice the log at record boundary
        auto pos_seq{indexes.upperBoundSequenceForPosition(lsn.position + adjusted_max_size)};
        if (pos_seq.key > 0)
        {
            assert(static_cast<uint64_t>(pos_seq.key) >= lsn.position + adjusted_max_size);
            return {sn_metadata, log->slice(lsn.position, pos_seq.key - lsn.position)};
        }
    }

    /// Slice to max
    return {sn_metadata, log->slice(lsn.position, max_to_fetch)};
}

/// trim to sn, returns bytes trimmed
int64_t LogSegment::trim(int64_t sn) /// NOLINT(readability-convert-member-functions-to-static)
{
    (void)sn;
    return 0;
}

int64_t LogSegment::readNextSequence()
{
    auto last_indexed_sn_position = indexes.lastIndexedSequencePosition();
    auto last_sn = last_indexed_sn_position.key;
    if (last_sn < 1)
        last_sn = 0;

    auto fetched_data = read(last_sn, log->size(), log->size(), last_indexed_sn_position.value);
    if (!fetched_data.isValid() || fetched_data.records == nullptr)
        return base_sn;

    LOG_DEBUG(
        logger,
        "Calculating log next sn for {}, last_sn={}, file_position={} file_size={}",
        log->getFilename().c_str(),
        last_sn,
        fetched_data.records->startPosition(),
        log->size());

    uint64_t read_records = 0;
    uint64_t read_bytes = 0;
    int64_t next_sn = base_sn;

    auto start = DB::MonotonicMilliseconds::now();

    fetched_data.records->applyRecordMetadata(
        [&](RecordPtr record, uint64_t) {
            next_sn = record->getSN() + 1;
            /// Since we would like to have the last batch's next sn, we always return false
            /// until it reaches the end of file
            read_records += 1;
            read_bytes += record->totalSerializedBytes();
            return false;
        },
        {});

    auto elapsed = DB::MonotonicMilliseconds::now() - start;

    LOG_DEBUG(
        logger,
        "End of calculating log next sn for {}, elapsed={}ms, read_records={}, read_bytes={}, last_sn={}, "
        "next_sn={}",
        log->getFilename().c_str(),
        elapsed,
        read_records,
        read_bytes,
        last_sn,
        next_sn);

    assert(next_sn >= base_sn);
    return next_sn;
}

void LogSegment::flush()
{
    /// Log sync can be very expensive: from hundred of milli-second to 10s of seconds
    log->sync();
    indexes.sync();
}

void LogSegment::close()
{
    indexes.close();
}

void LogSegment::remove()
{
    const auto & segment_file = filename();
    const auto & index_file = indexes.indexDir();

    LOG_INFO(logger, "Removing segment={} seg_indexes={} base_sn={}", segment_file.c_str(), index_file.c_str(), base_sn);
    {
        std::error_code ec;
        fs::remove_all(segment_file, ec);

        if (ec)
            LOG_INFO(logger, "Failed to remove segment={} base_sn={}, error={}", segment_file.c_str(), base_sn, ec.message());
    }
    {
        std::error_code ec;
        fs::remove_all(index_file, ec);
        if (ec)
            LOG_INFO(logger, "Failed to remove seg_indexes={} base_sn={}, error={}", index_file.c_str(), base_sn, ec.message());
    }
    LOG_INFO(logger, "Successfully removed segment={} seg_indexes={} base_sn={}", segment_file.c_str(), index_file.c_str(), base_sn);
}

void LogSegment::changeFileSuffix(const std::string & new_suffix)
{
    const auto & file = filename();
    if (file.extension() != new_suffix)
    {
        std::error_code err;

        fs::path new_file(file);
        new_file += new_suffix;
        log->renameTo(new_file, err); /// clang-tidy: bugprone-use-after-move

        if (err)
            LOG_ERROR(logger, "Failed to rename file from {} to {}, error={}", file.c_str(), new_file.c_str(), err.message());
    }

    const auto & index_dir = indexes.indexDir();
    if (index_dir.extension() != new_suffix)
    {
        std::error_code err;
        fs::path new_index_dir(index_dir);
        new_index_dir += new_suffix;
        indexes.renameTo(new_index_dir, err); /// clang-tidy: bugprone-use-after-move

        if (err)
            LOG_ERROR(logger, "Failed to rename file from {} to {}, error={}", index_dir.c_str(), new_index_dir.c_str(), err.message());
    }
}

bool LogSegment::shouldRoll(const LogConfig & config_, uint32_t records_size) const
{
    /// FIXME, timestamp + jitter based rolling policy
    return log->size() + records_size > static_cast<uint64_t>(config_.segment_size);
}

void LogSegment::turnInactive()
{
    indexes.index(max_etimestamp_and_sn_so_far, max_atimestamp_and_sn_so_far);
}

void LogSegment::updateParentDir(const fs::path & parent_dir)
{
    log->updateParentDir(parent_dir);
    indexes.updateParentDir(parent_dir);
}

FileRecords::LogSequencePosition LogSegment::translateSequence(int64_t sn, int64_t starting_file_position)
{
    auto entry = indexes.lowerBoundPositionForSequence(sn);
    if (entry.key == sn)
        return {sn, static_cast<uint64_t>(entry.value), 0};

    return log->searchForSequenceWithSize(sn, std::max(entry.value, starting_file_position));
}
}
