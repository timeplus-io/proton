#pragma once

#include <NativeLog/Base/AppendOnlyFile.h>
#include <NativeLog/Schemas/MemoryRecords.h>

#include <boost/noncopyable.hpp>

#include <atomic>

namespace nlog
{
/// FileRecords is backed by a file. FileRecords generally maintains a file handle
/// which can be written / appended to by a single thread, but can be read concurrently
/// by multiple threads
/// When multiple threads like to read a portion of the file concurrently, each thread
/// shall have its own `slice` by providing a slice range (start_pos, end_pos)
class FileRecords final : private boost::noncopyable
{
public:
    static std::shared_ptr<FileRecords> open(const fs::path & filename, bool file_already_exists, bool readonly);

    FileRecords(AppendOnlyFilePtr file, uint64_t start_pos_, uint64_t end_pos_, bool is_slice_);

    /// Append records to the file. Not thread safe
    /// @param records The records to append
    /// @return the number of bytes written to the underlying file
    int64_t append(const MemoryRecords & records);

    /// Read log records into the given buffer until the specified number of bytes read or
    /// the end of the file is reached
    /// @param dst Target buffer to copy the data to
    /// @param count Number of bytes to read
    /// @param position Offset in the file to read from
    int64_t read(uint8_t * dst, size_t count, uint32_t position);

    /// Return a slice of records from this instance, which is a view into this record set
    /// starting from the given position and with the given size limit
    /// If the size is beyond the end of the file, the end will be based on the size of the
    /// file at the time of the read.
    /// If the current instance is already sliced, the position will be taken relative to that
    /// slicing
    /// @param position The start position to begin read from
    /// @param size_ Teh number of bytes after the start position to include
    /// @return A sliced wrapper on this record set limited based on the given position and size
    std::shared_ptr<FileRecords> slice(uint32_t position, uint32_t size_);

    size_t size() const { return bytes; }

    void sync() const { file->sync(true); }

    const fs::path getFilename() const { return file->getFilename(); }

    uint64_t startPosition() const { return start_pos; }

public:
    struct LogOffsetPosition
    {
        int64_t offset;
        uint64_t position;
        uint64_t size;

        LogOffsetPosition(int64_t offset_, uint64_t position_, uint64_t size_) : offset(offset_), position(position_), size(size_) { }

        bool valid() const { return offset >= 0; }
    };

    /// Search forward for the file position of the last offset that is greater than or equal to the target offset
    /// and return the physical position and the size of the message at the returned offset
    /// If no such record batch is found, an invalid LogOffsetPosition will be returned
    LogOffsetPosition searchForOffsetWithSize(int64_t target_offset, uint64_t starting_position);

    /// Apply reads the RecordBatch from the underling file starting from the specified start position
    /// if provided or start from the FileRecords' internal start position in a streaming way.
    /// It calls the callback for each RecordBatch until the callback returns `true` which means stopping further
    /// processing or reach the end of the file or internal end position is reached.
    /// The callback is passed with a parsed MemoryRecords batch and the corresponding physical position
    /// After the callback, the memory in MemoryRecords will be recycled / re-used. So if callback needs to
    /// save the MemoryRecord somewhere for late use, a deep copy is needed
    void apply(
        std::function<bool(const MemoryRecords &, uint64_t)> callback,
        std::optional<uint64_t> starting_position,
        size_t bufsize = 1024 * 1024);

private:
    size_t availableBytes(uint32_t start_pos_, uint32_t size_) const;

private:
    AppendOnlyFilePtr file;
    /// Start position of the physical file
    uint64_t start_pos;
    /// End position of the physical file
    uint64_t end_pos;

    bool is_slice;

    std::atomic<size_t> bytes;
};

using FileRecordsPtr = std::shared_ptr<FileRecords>;
}
