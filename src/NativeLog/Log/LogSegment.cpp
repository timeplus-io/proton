#include "LogSegment.h"
#include "Loglet.h"

#include <NativeLog/Common/LogAppendDescription.h>
#include <base/logger_useful.h>

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
    , index_interval_bytes(log_config.index_interval_bytes)
    , log(FileRecords::open(Loglet::logFile(log_dir, base_sn_, file_suffix), file_already_exists, false))
    , max_etimestamp_and_sn_so_far(-1, -1)
    , max_atimestamp_and_sn_so_far(-1, -1)
    , indexes(Loglet::indexFileDir(log_dir, base_sn, file_suffix), base_sn_, logger_)
    , logger(logger_)
{
    max_etimestamp_and_sn_so_far = indexes.lastIndexedEventTimeSequence();
    max_atimestamp_and_sn_so_far = indexes.lastIndexedAppendTimeSequence();
}

LogSegment::~LogSegment()
{
}

int64_t LogSegment::append(const ByteVector & record, const LogAppendDescription & append_info)
{
    auto physical_position = log->size();
    if (physical_position == 0)
        rolling_based_timestamp = append_info.max_event_timestamp;

    LOG_TRACE(
        logger,
        "Appending {} bytes at end sn {} at file position {} with max event timestamp {}",
        record.size(),
        append_info.seq_metadata.record_sn,
        physical_position,
        append_info.max_event_timestamp);

    /// ensureSNInRange(largest_sn);

    /// Append the messages
    auto n = log->append(record);

    LOG_TRACE(logger, "Appended {} bytes to {} at end sn {}", n, log->getFilename().c_str(), append_info.seq_metadata.record_sn);

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

FetchDataDescription LogSegment::read(int64_t start_sn, int64_t max_size, std::optional<int64_t> position)
{
    FileRecords::LogSequencePosition lsn{start_sn, 0, 1};
    if (!position || *position < 0 || log->size() < static_cast<uint64_t>(*position))
    {
        lsn = translateSequence(start_sn);
        if (!lsn.valid())
            return {};
    }
    else
        lsn.position = *position;

    LogSequenceMetadata sn_metadata(start_sn, baseSequence(), lsn.position);

    auto adjusted_max_size = std::max<uint64_t>(max_size, lsn.size);
    if (adjusted_max_size == 0)
        return {sn_metadata, {}};

    /// We like to slide the log at record boundary
    auto pos_seq{indexes.upperBoundSequenceForPosition(lsn.position + adjusted_max_size)};
    if (pos_seq.key > 0)
    {
        assert(static_cast<uint64_t>(pos_seq.key) > lsn.position + adjusted_max_size);
        return {sn_metadata, log->slice(lsn.position, pos_seq.key - lsn.position)};
    }

    /// Slice to the end of file, use size=0
    return {sn_metadata, log->slice(lsn.position, 0)};
}

/// trim to sn, returns bytes trimmed
int64_t LogSegment::trim(int64_t sn)
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

    auto fetched_data = read(last_sn, log->size(), last_indexed_sn_position.value);
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
    /// log->close();
    indexes.close();
}

void LogSegment::remove()
{
}

void LogSegment::changeFileSuffix(const std::string & old_suffix, const std::string & new_suffix)
{
    (void)old_suffix;
    (void)new_suffix;
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
    (void)parent_dir;
}

FileRecords::LogSequencePosition LogSegment::translateSequence(int64_t sn, int64_t starting_file_position)
{
    auto entry = indexes.lowerBoundPositionForSequence(sn);
    if (entry.key == sn)
        return {sn, static_cast<uint64_t>(entry.value), 0};

    return log->searchForSequenceWithSize(sn, std::max(entry.value, starting_file_position));
}
}
