#pragma once

#include "Stds.h"

#include <sys/types.h>

#include <span>
#include <memory>

namespace nlog
{
/// AppendOnlyFile is a file class for reading, appending, mapping a file
/// AppendOnlyFile is multi-thread safe for reading and it doesn't buffering any read data
/// AppendOnlyFile is not multi-thread safe for writing and it doesn't buffering any write data.
/// It is simplified / tailed / optimized for NativeLog append-only use cases only
class AppendOnlyFile final
{
public:
    /// @param filename_ the backing file name of the channel
    AppendOnlyFile(const fs::path & filename_, bool file_already_exists, bool read_only_);
    ~AppendOnlyFile();

    /// Append a sequence of bytes to this channel from the given data buffer
    /// The file is grown if necessary to accommodate the written bytes, and then the file offset
    /// is updated with the number of bytes actually written.
    int64_t append(const void * data, uint64_t count);

    int64_t append(std::span<uint8_t> data);

    /// Reads a sequence of bytes from this channel into the given buffer, starting
    /// from the given file offset.
    /// This method doesn't modify this channel's offset nor the file descriptor underlying
    /// @param dst The target buffer into which bytes are copied
    /// @param count The maximum number of bytes to read
    /// @param offset The starting offset of the current channel
    /// @return The number of bytes read, possibly zero, or -1 if the given offset is greater
    /// than or equal to the file's current size
    int64_t read(void * dst, uint64_t count, uint64_t offset);

    int64_t read(std::span<uint8_t> dst, uint64_t offset);

    /// Transfer bytes from this channel's file to the given writable target file descriptor
    /// for example a socket or another file descriptor. The underlying implementation will use
    /// Linux sendfile / splice / io_uring system calls to avoid data copy between the source file
    /// and the target file. If the given offset is greater than the file's current size, then no
    /// bytes are transferred.
    /// This method doesn't modify the channel's offset nor the offset of the underlying file
    /// descriptor. It will update the offset of the target file descriptor
    /// @param offset Start offset of the current channel file
    /// @param count The maximum number of bytes to be copied.
    /// @param target_fd The target file descriptor to copy to
    /// @return The number of bytes, possibly zero, that were actually copied
    int64_t zeroCopyTo(uint64_t offset, uint64_t count, int32_t target_fd);

    /// Transfer bytes into this channel's file from the source file descriptor.
    /// The method may or may not transfer all of the requested bytes. If the given offset is
    /// greater than the source file's current size, then no bytes are transferred.
    /// This method doesn't modify this channel's offset or the offset of underlying file
    /// descriptor. It will update the offset of the source file descriptor
    /// @param source_fd The source file descriptor to copy from
    /// @param offset The starting offset of the source file descriptor
    /// @param count The maximum number of bytes to be copied from
    /// @return The number of bytes, possibly zero, that were actually copied
    int64_t zeroCopyFrom(int32_t source_fd, uint64_t offset, uint64_t count);

    /// Returns the current size of the channel's backing file in bytes
    int64_t size() const;

    /// Flush any updates to this channel's file to be written to the storage device
    /// that contains it
    /// @param include_metadata If true, data and metadata of the underlying file will
    /// be flushed, otherwise only data is flushed
    void sync(bool include_metadata) const;

    const fs::path & getFilename() const { return filename; }

private:
    fs::path filename;
    bool read_only;
    int32_t fd;
};

using AppendOnlyFilePtr = std::shared_ptr<AppendOnlyFile>;
}
