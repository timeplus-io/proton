#pragma once

#include "IndexEntry.h"
#include "Indexes.h"
#include "LogConfig.h"

#include <NativeLog/Base/AppendOnlyFile.h>
#include <NativeLog/Base/Stds.h>
#include <NativeLog/Common/FetchDataDescription.h>
#include <NativeLog/Common/FileRecords.h>

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
struct LogAppendDescription;

/// A segment of `Log`. Each segment has 2 components: log and indexes
/// `log` is backed by a local append-only binary file which contains the actual messages
/// `indexes` contain
/// - sn to physical file position mapping index
/// - physical file position to sn mapping index
/// - event time to sn mapping index
/// - append time to sn mapping index
/// Each segment has a base sn with is an sn <= the least sn of any message
/// in this segment and > any sn in any previous segment.
/// A segment with a base sn [base_sn] would be stored in 2 files:
/// - a [base_sn].log
/// - a [base_sn].index
class LogSegment final : private boost::noncopyable
{
public:
    LogSegment(
        const fs::path & log_dir,
        int64_t base_sn,
        const LogConfig & log_config,
        Poco::Logger * logger,
        bool file_already_exists = false,
        bool preallocate = false,
        const std::string & file_suffix = "");

    /// Append the given messages starting with the given sn. Add one entry to the index if needed
    /// Append is not multi-thread safe so it is assumed this method is being called from within a lock
    /// @param record The log entries to append
    /// @param append_info Append info which carries record information
    /// @return the physical position in the file of the appended records
    int64_t append(const ByteVector & record, const LogAppendDescription & append_info);

    /// Read a message set from this segment starting with the first sn >= start_sn.
    /// The message set will include no more than max_size bytes and will end before max_sn if a
    /// max_sn is specified.
    /// Read is multi-thread safe
    /// @param start_sn Logic record sn to read from
    /// @param max_size maximum size to read
    /// @param max_position The max file position in the log which shall be exposed for read
    /// @param position File position for sn if set
    /// @return The fetched data and the sn metadata of the first message whose sn is >= start_sn
    /// or empty if the start_sn is larger than the largest sn in this log
    FetchDataDescription read(int64_t start_sn, uint64_t max_size, uint64_t max_position, std::optional<uint64_t> position);

    /// Trim to sn, returns bytes trimmed
    int64_t trim(int64_t sn);

    void flush();

    void close();

    /// Delete this log segment from the file system
    void remove();

    bool shouldRoll(const LogConfig & config, uint32_t records_size) const;

    /// Physical size in bytes
    uint64_t size() const { return log->size(); }

    int64_t lastModified() const { return log->lastModified(); }

    const fs::path & filename() const { return log->getFilename(); }

    int64_t baseSequence() const { return base_sn; }

    /// Calculate the sn that would be used for next record to be appended to this
    /// segment. It is expensive as it needs replay from the last sn checkpoint
    /// and it is usually called during system startup
    /// This method is multi-thread safe
    int64_t readNextSequence();

    /// Run recovery. This will rebuild the index from the log file and lop off
    /// any invalid bytes from the end of the log and index
    /// @return The nunber of bytes truncated from the log
    size_t recover() { return 0; }

    void updateParentDir(const fs::path & parent_dir);

    bool sanityCheck() { return true; }

    /// Change the suffix for the index and log files for this log segment
    /// Exception shall be handled by caller
    void changeFileSuffix(const std::string & new_suffix);

    /// When LogSegment turns from active segment to inactive (rolled)
    /// Append the largest time index entry to the indexes. The time index
    /// entry appended will be used to decide when to delete the segment
    void turnInactive();

    const Indexes & getIndexes() const { return indexes; }

    TimestampSequence maxEventTimestampSequence() const { return max_etimestamp_and_sn_so_far; }
    TimestampSequence maxAppendTimestampSequence() const { return max_atimestamp_and_sn_so_far; }

private:
    /// Find the physical file position for the first message with sn >= the request sn
    /// The starting_file_position argument is an optimization that can be used if we already know a
    /// valid starting position in the file higher than the greatest-lower-bound from the index
    /// @param sn The stream sn want to translate
    /// @param starting_file_position A lower bound on the file position from which to begin the search
    /// @return The physical position in the log storing the record with the least sn >= the requested sn and the size
    ///         of the record
    FileRecords::LogSequencePosition translateSequence(int64_t sn, int64_t starting_file_position = 0);

    void index(int64_t largest_sn, int64_t physical_position);

private:
    int64_t base_sn;
    int32_t max_index_size;
    int32_t index_interval_bytes;
    int32_t bytes_since_last_index_entry = 0;
    /// roll jitter

    FileRecordsPtr log;

    std::atomic<int64_t> rolling_based_timestamp;

    /// FIXME, multi-thread sync
    TimestampSequence max_etimestamp_and_sn_so_far;
    TimestampSequence max_atimestamp_and_sn_so_far;

    Indexes indexes;

    Poco::Logger * logger;
};

using LogSegmentPtr = std::shared_ptr<LogSegment>;
}
