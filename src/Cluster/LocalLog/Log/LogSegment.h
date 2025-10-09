#pragma once

#include <Cluster/LocalLog/Log/LogConfig.h>

#include <Cluster/LocalLog/Base/AppendOnlyFile.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/LocalLog/Common/FileLogEntries.h>

#include <boost/noncopyable.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace cluster::nlog
{
struct LogConfig;
struct SequencePosition;

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
        uint64_t segment_id_,
        int64_t base_sn_,
        LogConfigPtr log_config,
        LoggerPtr logger_,
        bool file_already_exists = false,
        const std::string & file_suffix = "");

    /// Append the given messages starting with the given sn. Add one entry to the index if needed
    /// Append is not multi-thread safe so it is assumed this method is being called from within a lock
    /// \param entry_data The serialized log entry to append
    /// \return error code
    CallResult<size_t> append(const std::vector<ByteVector> & entry_data, size_t total_bytes);

    CallResult<size_t> append(const ByteVector & entry_data);

    FileLogEntriesPtr fetch(uint64_t file_position, uint64_t size_) const { return log->slice(file_position, size_); }

    /// Trim log tail [file_position, log_end], returns bytes trimmed
    CallResult<uint64_t> trim(uint64_t file_position);

    /// \param incremental if it true, use file system's incremental flush API to flush data if appropriate
    /// otherwise call `fsync` to do full data flush
    int flush(bool incremental);

    /// Delete this log segment from the file system
    int remove();

    bool shouldRoll(const LogConfigPtr & config_, uint32_t append_size) const noexcept
    {
        /// FIXME, timestamp + jitter based rolling policy
        return log->size() + append_size > config_->segment_size;
    }

    /// Physical size in bytes
    uint64_t size() const noexcept { return log->size(); }

    /// Flushed bytes (to file system)
    uint64_t syncSize() const noexcept { return last_sync_offset; }

    /// Physical size in bytes of the file, size() shall always equal to fsize()
    CallResult<uint64_t> fsize() const;

    CallResult<int64_t> lastModified() const;

    const fs::path & filename() const { return log->getFilename(); }

    int64_t baseSequence() const noexcept { return base_sn; }

    uint64_t getID() const noexcept { return id; }

    /// Calculate the sn that would be used for next log entry to be appended to this
    /// segment. It can be expensive as it needs replay from the last sn checkpoint
    /// and it is usually called during system startup when there is an unclean shutdown
    /// We assume log is intact
    /// This method is multi-thread safe
    int64_t readNextSequence(uint64_t start_position, int64_t start_sn) const;

    /// Run recovery. This will rebuild the index from the log file and lop off
    /// any invalid bytes from the end of the log and index
    /// \return The {next_sn, trimmed_bytes} pair
    /// if `trimmed_bytes` is not zero, it means the tail log entry has been corrupted and truncated.
    CallResult<std::pair<int64_t, size_t>> recover(const SequencePosition & last_sn_pos);

    void updateParentDir(const fs::path & parent_dir) { log->updateParentDir(parent_dir); }

    bool sanityCheck() { return true; }

    /// Change the suffix for the index and log files for this log segment
    int changeFileSuffix(const std::string & new_suffix);

    /// When LogSegment turns from active segment to inactive (rolled)
    /// Append the largest time index entry to the indexes. The time index
    /// entry appended will be used to decide when to delete the segment
    void turnInactive();

    /// Find the physical file position for the first message with sn >= the request sn
    /// The \param start_position argument is an optimization that can be used if we already know a
    /// valid starting position in the file higher than the greatest-lower-bound from the index
    /// \param sn The stream sn want to translate
    /// \return The physical position in the log storing the log entry with the least sn >= the
    /// requested sn and the size of the entry
    std::optional<FileLogEntries::LogSequencePosition> translateSequence(int64_t sn, uint64_t start_position) const
    {
        return log->searchForSequenceWithSize(sn, start_position);
    }

private:
    std::pair<int64_t, uint64_t> readNextSequenceAndCheckCorruption(uint64_t start_position, int64_t start_sn, bool & corrupted) const;

private:
    /// Log segment id  is monotonically increased
    const int64_t id;
    const int64_t base_sn;

    uint64_t last_sync_offset = 0;

    FileLogEntriesPtr log;

    /// roll jitter
    LoggerPtr logger;
};

using LogSegmentPtr = std::shared_ptr<LogSegment>;
}
