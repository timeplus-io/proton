#pragma once

#include <Cluster/LocalLog/Base/AppendOnlyFile.h>
#include <Cluster/Common/Entry.h>

#include <atomic>

namespace cluster::nlog
{
class FileLogEntries;
using FileLogEntriesPtr = std::shared_ptr<FileLogEntries>;

/// FileLogEntries is backed by a file. FileLogEntries generally maintains a file handle
/// which can be written / appended to by a single thread, but can be read concurrently
/// by multiple threads
/// When multiple threads like to read a portion of the file concurrently, each thread
/// shall have its own `slice` by providing a slice range (start_pos, end_pos)
class FileLogEntries final
{
public:
    static std::shared_ptr<FileLogEntries>
    open(const fs::path & filename, bool file_already_exists, bool readonly, uint64_t initial_file_size, bool preallocate);

    FileLogEntries(AppendOnlyFilePtr file, uint64_t start_pos_, uint64_t end_pos_, bool is_slice_);

    /// Append log entries to the file. Not thread safe
    /// \param batch_data The entries data to append
    /// \return the number of bytes written to the underlying file or error
    CallResult<size_t> append(const std::vector<ByteVector> & batch_data, size_t total_bytes)
    {
        auto written = file->append(batch_data, total_bytes);
        bytes += written.result;
        return written;
    }

    /// Append log entry to the file. Not thread safe
    /// \param entry_data The entry data to append
    /// \return the number of bytes written to the underlying file or error
    CallResult<size_t> append(const ByteVector & entry_data)
    {
        auto written = file->append(entry_data.data(), entry_data.size());
        bytes += written.result;
        return written;
    }

    /// Read log entries into the given buffer until the specified number of bytes read or
    /// the end of the file is reached. Throw exception if an error happens during read
    /// \param dst Target buffer to copy the data to
    /// \param bytes_to_read Number of bytes to read
    /// \param position Offset in the file to read from
    CallResult<uint64_t> read(char * dst, size_t bytes_to_read, uint64_t position);

    /// Return a slice of log entries from this instance, which is a view into this log entry set
    /// starting from the given position and with the given size limit
    /// If the size is beyond the end of the file, the end will be based on the size of the
    /// file at the time of the read.
    /// If the current instance is already sliced, the position will be taken relative to that
    /// slicing
    /// \param position The start position to begin read from
    /// \param size_ The number of bytes after the start position to include
    /// \return A sliced wrapper on this log entry set limited based on the given position and size
    std::shared_ptr<FileLogEntries> slice(uint64_t position, uint64_t size_);

    size_t size() const { return bytes; }

    CallResult<int64_t> lastModified() const { return file->lastModified(); }

    int sync() const { return file->sync(true); }

    /// \return 0 if success, otherwise error code
    int syncTail(uint64_t offset) const { return file->syncTail(offset); }

    CallResult<uint64_t> trim(uint64_t length)
    {
        assert(bytes > length);

        if (auto err = file->truncate(length); err != 0)
            return CallResult<uint64_t>{0, err};

        uint64_t truncated = bytes - length;
        bytes = length;

        assert(size() == fsize().result);

        return CallResult<uint64_t>{truncated, 0};
    }

    const fs::path & getFilename() const { return file->getFilename(); }

    CallResult<uint64_t> fsize() const { return file->size(); }

    void renameTo(fs::path new_file, std::error_code & err) { file->renameTo(std::move(new_file), err); }

    void updateParentDir(const fs::path & parent_dir) { file->updateParentDir(parent_dir); }

    uint64_t startPosition() const { return start_pos; }
    uint64_t endPosition() const { return end_pos; }

    /// If the current slice reach the end of the sealed segment file
    void setEndOfFile()
    {
        assert(bytes == end_pos);
        eof = true;
    }

    bool reachedEndOfFile() const { return eof; }

    int close() const
    {
        if (is_slice)
            return 0;

        if (file)
            return file->close();

        return 0;
    }

public:
    struct LogSequencePosition
    {
        int64_t sn;
        uint64_t file_position;
        uint64_t entry_size;

        LogSequencePosition(int64_t sn_, uint64_t file_position_, uint64_t entry_size_)
            : sn(sn_), file_position(file_position_), entry_size(entry_size_)
        {
        }
    };

    /// Search forward for the file position of the last offset that is greater than or equal to the target offset
    /// and return the physical position and the size of the message at the returned offset
    /// If no such log entry batch is found, {} will be returned
    std::optional<LogSequencePosition> searchForSequenceWithSize(int64_t target_sn, uint64_t starting_position);

    /// Apply reads the log entries from the underling file starting from the specified start position
    /// if provided or start from the FileLogEntries' internal start position in a streaming way.
    /// It calls the callback for each log Entry until the callback returns `true` which means stopping further
    /// reading / processing or reach the end of the file or internal end position is reached.
    /// The callback is passed with a partial parsed Entry (only metadata is parsed) and the corresponding physical position
    void
    applyLogEntryMetadata(std::function<bool(cluster::EntryPtr, uint64_t)> callback, std::optional<uint64_t> starting_position) const;

    /// Deserialize data until sn == max_sn or EOF or end_pos has been reached or \max_size exceeded
    /// NOTE: the \max_size is advisory and it will always return at least one entry even if it exceeds the max_size limit.
    /// \return {parsed_bytes, entries} pair
    CallResult<std::pair<uint64_t, cluster::EntryPtrs>>
    deserialize(int64_t max_sn, size_t max_size, uint64_t max_buffer_size = 16 * 1024 * 1024ull) const;

    bool empty() const { return !file || start_pos == end_pos; }

private:
    size_t availableBytes(uint64_t start_pos_, uint64_t size_) const;

private:
    AppendOnlyFilePtr file;
    /// Start position of the physical file
    uint64_t start_pos;
    /// End position of the physical file
    uint64_t end_pos;

    bool is_slice;
    bool eof = false;

    std::atomic<uint64_t> bytes;
};
}
