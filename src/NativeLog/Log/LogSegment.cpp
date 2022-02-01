#include "LogSegment.h"
#include "Loglet.h"

#include <base/logger_useful.h>

namespace nlog
{
LogSegment::LogSegment(
    const fs::path & log_dir,
    int64_t base_offset_,
    const LogConfig & log_config,
    Poco::Logger * logger_,
    bool file_already_exists,
    bool /*preallocate*/,
    const std::string & file_suffix)
    : base_offset(base_offset_)
    , max_index_size(log_config.max_index_size)
    , index_interval_bytes(log_config.index_interval_bytes)
    , log(FileRecords::open(Loglet::logFile(log_dir, base_offset_, file_suffix), file_already_exists, false))
    , max_etimestamp_and_offset_so_far(-1, -1)
    , max_atimestamp_and_offset_so_far(-1, -1)
    , indexes(Loglet::indexFileDir(log_dir, base_offset, file_suffix), base_offset_, logger_)
    , logger(logger_)
{
    max_etimestamp_and_offset_so_far = indexes.lastIndexedEventTimeOffset();
    max_atimestamp_and_offset_so_far = indexes.lastIndexedAppendTimeOffset();
}

LogSegment::~LogSegment()
{
}

int64_t
LogSegment::append(int64_t largest_offset, int64_t largest_timestamp, int64_t offset_of_max_timestamp, const MemoryRecords & records)
{
    auto physical_position = log->size();
    if (physical_position == 0)
        rolling_based_timestamp = largest_timestamp;

    LOG_TRACE(
        logger,
        "Inserting {} bytes at end offset {} at file position {} with largest timestamp {} at offset {}",
        records.sizeInBytes(),
        largest_offset,
        physical_position,
        largest_timestamp,
        offset_of_max_timestamp);

    /// ensureOffsetInRange(largest_offset);

    /// Append the messages
    auto n = log->append(records);
    LOG_TRACE(logger, "Appended {} bytes to {} at end offset {}", n, log->getFilename().c_str(), largest_offset);

    /// Update the in memory max timestamp and corresponding offset.
    if (largest_timestamp > max_etimestamp_and_offset_so_far.key)
        max_etimestamp_and_offset_so_far = TimestampOffset(largest_timestamp, offset_of_max_timestamp);

    auto append_timestamp = records.appendTimestamp();
    if (append_timestamp > max_atimestamp_and_offset_so_far.key)
        max_atimestamp_and_offset_so_far = TimestampOffset(append_timestamp, records.baseOffset());

    if (bytes_since_last_index_entry > index_interval_bytes)
    {
        indexes.index(largest_offset, physical_position, max_etimestamp_and_offset_so_far, max_atimestamp_and_offset_so_far);
        bytes_since_last_index_entry = 0;
    }

    bytes_since_last_index_entry += records.sizeInBytes();
    return physical_position;
}

FetchDataInfo LogSegment::read(int64_t start_offset, uint64_t max_size)
{
    auto start_offset_and_size = translateOffset(start_offset);
    if (!start_offset_and_size.valid())
        return {};

    LogOffsetMetadata offset_metadata(start_offset, baseOffset(), start_offset_and_size.position);

    auto adjusted_max_size = std::max<int64_t>(max_size, start_offset_and_size.size);
    assert(adjusted_max_size > 0);

    return {offset_metadata, log->slice(start_offset_and_size.position, adjusted_max_size)};
}

/// trim to offset, returns bytes trimmed
int64_t LogSegment::trim(int64_t offset)
{
    (void)offset;
    return 0;
}

int64_t LogSegment::readNextOffset()
{
    auto last_indexed_offset_position = indexes.lastIndexedOffsetPosition();
    auto last_offset = last_indexed_offset_position.key;
    if (last_offset < 1)
        last_offset = 0;

    auto fetched_data = read(last_offset, log->size());
    if (!fetched_data.isValid() || fetched_data.records == nullptr)
        return base_offset;

    LOG_DEBUG(
        logger,
        "Calculating log next offset for {}, last_offset={}, file_position={} file_size={}",
        log->getFilename().c_str(),
        last_offset,
        fetched_data.records->startPosition(),
        log->size());

    uint64_t read_records = 0;
    uint64_t read_bytes = 0;
    int64_t next_offset = base_offset;

    auto start = DB::MonotonicMilliseconds::now();

    fetched_data.records->apply(
        [&](const MemoryRecords & records, uint64_t) {
            next_offset = records.nextOffset();
            /// Since we would like to have the last batch's next offset, we always return false
            /// until it reaches the end of file
            read_records += records.size();
            read_bytes += records.sizeInBytes();
            return false;
        },
        {});

    auto elapsed = DB::MonotonicMilliseconds::now() - start;

    LOG_DEBUG(
        logger,
        "End of calculating log next offset for {}, elapsed={}ms, read_records={}, read_bytes={}, last_offset={}, "
        "next_offset={}",
        log->getFilename().c_str(),
        elapsed,
        read_records,
        read_bytes,
        last_offset,
        next_offset);

    assert(next_offset >= base_offset);
    return next_offset;
}

void LogSegment::flush()
{
    log->sync();
    indexes.sync();
}

void LogSegment::close()
{
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
    return log->size() + records_size > config_.segment_size;
}

void LogSegment::turnInactive()
{
    indexes.index(max_etimestamp_and_offset_so_far, max_atimestamp_and_offset_so_far);
}

void LogSegment::updateParentDir(const fs::path & parent_dir)
{
    (void)parent_dir;
}

FileRecords::LogOffsetPosition LogSegment::translateOffset(int64_t offset, uint64_t starting_file_position)
{
    auto entry = indexes.lowerBoundOffsetPosition(offset);
    return log->searchForOffsetWithSize(offset, std::max<int64_t>(entry.value, starting_file_position));
}
}
