#pragma once

#include "IndexEntry.h"
#include "Indexes.h"
#include "LogConfig.h"

#include <NativeLog/Base/AppendOnlyFile.h>
#include <NativeLog/Base/Stds.h>
#include <NativeLog/Common/FetchDataInfo.h>
#include <NativeLog/Common/FileRecords.h>
#include <NativeLog/Common/LogAppendInfo.h>
#include <NativeLog/Schemas/MemoryRecords.h>

#include <boost/noncopyable.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace Poco
{
class Logger;
}

namespace nlog
{
struct LogConfig;

/// A segment of `Log`. Each segment has 2 components: log and indexes
/// `log` is backed by a local append-only binary file which contains the actual messages
/// `indexes` contain
/// - offset to physical file position mapping index
/// - physical file position to offset mapping index
/// - event time to offset mapping index
/// - append time to offset mapping index
/// Each segment has a base offset with is an offset <= the least offset of any message
/// in this segment and > any offset in any previous segment.
/// A segment with a base offset [base_offset] would be stored in 2 files:
/// - a [base_offset].log
/// - a [base_offset].index
class LogSegment final : private boost::noncopyable
{
public:
    LogSegment(
        const fs::path & log_dir,
        int64_t base_offset,
        const LogConfig & log_config,
        Poco::Logger * logger,
        bool file_already_exists = false,
        bool preallocate = false,
        const std::string & file_suffix = "");
    ~LogSegment();

    /// Append the given messages starting with the given offset. Add one entry to the index if needed
    /// Append is not multi-thread safe so it is assumed this method is being called from within a lock
    /// @param largest_offset The last offset in the records
    /// @param largest_timestamp The largest event timestamp in the records
    /// @param offset_of_max_timestamp The largest timestamp in the records
    /// @param records The log entries to append
    /// @return the physical position in the file of the appended records
    int64_t append(int64_t largest_offset, int64_t largest_timestamp, int64_t offset_of_max_timestamp, const MemoryRecords & records);

    /// Read a message set from this segment starting with the first offset >= start_offset.
    /// The message set will include no more than max_size bytes and will end before max_offset if a
    /// max_offset is specified.
    /// Read is multi-thread safe
    /// @start_offset Logic record offset to read from
    /// @max_size maximum size to read
    /// @return The fetched data and the offset metadata of the first message whose offset is >= start_offset
    /// or empty if the start_offset is larger than the largest offset in this log
    FetchDataInfo read(int64_t start_offset, uint64_t max_size);

    /// Trim to offset, returns bytes trimmed
    int64_t trim(int64_t offset);

    void flush();

    void close();

    /// Delete this log segment from the file system
    void remove();

    bool shouldRoll(const LogConfig & config, uint32_t records_size) const;

    /// Physical size in bytes
    int64_t size() const { return log->size(); }

    int64_t baseOffset() const { return base_offset; }

    /// Calculate the offset that would be used for next message to be appended to this
    /// segment. It is expensive as it needs replay from the last offset checkpoint
    /// and it is usually called during system startup
    /// This method is multi-thread safe
    int64_t readNextOffset();

    int32_t recover() { return 0; }

    void updateParentDir(const fs::path & parent_dir);

    bool sanityCheck() { return true; }

    /// Change the suffix for the index and log files for this log segment
    /// Exception shall be handled by caller
    void changeFileSuffix(const std::string & old_suffix, const std::string & new_suffix);

    /// When LogSegment turns from active segment to inactive (rolled)
    /// Append the largest time index entry to the indexes. The time index
    /// entry appended will be used to decide when to delete the segment
    void turnInactive();

private:
    /// Find the physical file position for the first message with offset >= the request offset
    /// The starting_file_position argument is an optimization that can be used if we already know a
    /// valid starting position in the file higher than the greatest-lower-bound from the index
    /// @param offset The logic offset want to translate
    /// @param starting_file_position A lower bound on the file position from which to begin the search
    /// @returearch The physical position in the log storing the message with the least offset >= the requested offset and the size
    /// of the message or
    FileRecords::LogOffsetPosition translateOffset(int64_t offset, uint64_t starting_file_position = 0);

    void index(int64_t largest_offset, int64_t physical_position);

private:
    int64_t base_offset;
    int32_t max_index_size;
    int32_t index_interval_bytes;
    int32_t bytes_since_last_index_entry = 0;
    /// roll jitter

    FileRecordsPtr log;

    std::atomic<int64_t> rolling_based_timestamp;

    /// FIXME, multi-thread sync
    TimestampOffset max_etimestamp_and_offset_so_far;
    TimestampOffset max_atimestamp_and_offset_so_far;

    Indexes indexes;

    Poco::Logger * logger;
};

using LogSegmentPtr = std::shared_ptr<LogSegment>;
}
