#include "AppendOnlyFile.h"

#include "Utils.h"

#include <Common/Exception.h>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int READONLY;
}
}

namespace nlog
{

namespace
{
    int32_t fileOpenFlags(bool file_already_exists, bool read_only)
    {
        if (read_only)
            return O_RDONLY;

        return file_already_exists ? O_APPEND | O_RDWR: O_APPEND | O_RDWR | O_CREAT;
    }
}

AppendOnlyFile::AppendOnlyFile(const fs::path & filename_, bool file_already_exists, bool read_only_) : filename(filename_), read_only(read_only_)
{
    if (file_already_exists)
        fd = ::open(filename.c_str(), fileOpenFlags(file_already_exists, read_only));
    else
        fd = ::open(filename.c_str(), fileOpenFlags(file_already_exists, read_only), 0666);

    if (fd == -1)
        DB::throwFromErrnoWithPath(
            "Cannot open file " + filename.string(),
            filename.string(),
            errno == ENOENT ? DB::ErrorCodes::FILE_DOESNT_EXIST : DB::ErrorCodes::CANNOT_OPEN_FILE);
}

AppendOnlyFile::~AppendOnlyFile()
{
    if (fd >= 0)
        ::close(fd);
}

int64_t AppendOnlyFile::append(const char * data, uint64_t bytes_to_write)
{
    if (read_only)
        throw DB::Exception(DB::ErrorCodes::READONLY, "File {} is readonly", filename.c_str());

    int64_t written_bytes = 0;
    while (bytes_to_write)
    {
        /// FIXME: io_uring
        auto n = ::write(fd, data + written_bytes, bytes_to_write);
        if (n >= 0)
        {
            written_bytes += n;
            bytes_to_write -= n;
        }
        else
        {
            if (errno != EINTR)
                DB::throwFromErrnoWithPath(
                    "Cannot write to file " + filename.string(), filename.string(), DB::ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

        }
    }

    return written_bytes;
}

int64_t AppendOnlyFile::append(std::span<char> data)
{
    return append(data.data(), data.size());
}

/// Keep reading until expected bytes have been read or EOF is reached
int64_t AppendOnlyFile::read(char * dst, uint64_t bytes_to_read, uint64_t offset)
{
    int64_t read_bytes = 0;
    while (bytes_to_read)
    {
        auto n = ::pread(fd, dst, bytes_to_read, offset + read_bytes);
        if (n > 0)
        {
            read_bytes += n;
            bytes_to_read -= n;
        }
        else if (n == 0 )
        {
            /// EOF
            break;
        }
        else
        {
            if (errno != EINTR)
                DB::throwFromErrnoWithPath(
                    "Cannot read from file " + filename.string(), filename.string(), DB::ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }
    }

    return read_bytes;
}

int64_t AppendOnlyFile::read(std::span<char> dst, uint64_t offset)
{
    return read(dst.data(), dst.size(), offset);
}

int64_t AppendOnlyFile::zeroCopyTo(uint64_t offset, uint64_t count, int32_t target_fd)
{
    (void)offset;
    (void)count;
    (void)target_fd;
    return 0;
}

int64_t AppendOnlyFile::zeroCopyFrom(int32_t source_fd, uint64_t offset, uint64_t count)
{
    (void)source_fd;
    (void)offset;
    (void)count;
    return 0;
}

int64_t AppendOnlyFile::size() const
{
    struct stat buf;
    int res = fstat(fd, &buf);
    if (-1 == res)
        DB::throwFromErrnoWithPath("Cannot execute fstat " + filename.string(), filename.string(), DB::ErrorCodes::CANNOT_FSTAT);

    return buf.st_size;
}

void AppendOnlyFile::sync(bool include_metadata) const
{
    flushFile(fd, include_metadata);
}
}
