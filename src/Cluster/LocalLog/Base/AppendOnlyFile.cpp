#include <Cluster/LocalLog/Base/AppendOnlyFile.h>

#include <IO/WriteHelpers.h>
#include <Common/CurrentMetrics.h>

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

namespace DB
{
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int OK;
}
}

namespace CurrentMetrics
{
extern const Metric LocalFileSystemShortWrite;
}

namespace cluster::nlog
{

namespace
{
int32_t fileOpenFlags(bool file_already_exists, bool read_only)
{
    if (read_only)
        return O_RDONLY;

    return file_already_exists ? O_APPEND | O_RDWR : O_APPEND | O_RDWR | O_CREAT;
}
}

AppendOnlyFile::AppendOnlyFile(const fs::path & filename_, uint64_t initial_file_size, bool preallocate)
    : AppendOnlyFile(filename_, fs::exists(filename_), /*read_only_=*/false, initial_file_size, preallocate)
{
}

AppendOnlyFile::AppendOnlyFile(
    const fs::path & filename_, bool file_already_exists, bool read_only_, uint64_t initial_file_size, bool preallocate)
    : filename(filename_), read_only(read_only_)
{
    if (file_already_exists)
        fd = ::open(filename.c_str(), fileOpenFlags(file_already_exists, read_only));
    else
        fd = ::open(filename.c_str(), fileOpenFlags(file_already_exists, read_only), 0666);

    if (fd == -1)
        DB::throwFromErrnoWithPath(
            "Cannot open file ",
            filename.string(),
            errno == ENOENT ? DB::ErrorCodes::FILE_DOESNT_EXIST : DB::ErrorCodes::CANNOT_OPEN_FILE,
            errno);

    if (!file_already_exists && !read_only && preallocate)
    {
        /// It is not a fatal error if fallocate failed
        /// On success, fallocate() returns zero.  On error, -1 is returned.
#if defined(OS_DARWIN)
        /// FIXME: Disable preallocate on mac. (there is a bug https://github.com/timeplus-io/proton-enterprise/issues/3636)
        (void)initial_file_size;

        /// Borrowed from https://github.com/trbs/fallocate/blob/e20f96cf50eed2371d4ae1c8270be71e6eef8511/fallocate/_fallocatemodule.c#L57-L70
        // fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, static_cast<off_t>(initial_file_size), 0};
        // auto res = ::fcntl(fd, F_PREALLOCATE, &store);
        // if (res == -1)
        // {
        //     // try and allocate space with fragments
        //     store.fst_flags = F_ALLOCATEALL;
        //     res = ::fcntl(fd, F_PREALLOCATE, &store);
        // }
        // if (res != -1)
        //     res = ::ftruncate(fd, initial_file_size);
#else
        [[maybe_unused]] auto res = ::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, initial_file_size);
        assert(res == 0);
#endif
    }
}

AppendOnlyFile::~AppendOnlyFile()
{
    close();
}

int AppendOnlyFile::close()
{
    if (fd >= 0)
    {
        auto ret = ::close(fd);
        fd = -1;

        return ret;
    }
    return 0;
}

CallResult<size_t> AppendOnlyFile::append(const std::vector<ByteVector> & batch_data, size_t total_bytes) const
{
    return append<ByteVector>(batch_data, total_bytes);
}

CallResult<size_t> AppendOnlyFile::append(const std::vector<std::span<char>> & batch_data, size_t bytes_to_write) const
{
    return append<std::span<char>>(batch_data, bytes_to_write);
}

CallResult<size_t> AppendOnlyFile::append(const std::vector<flatbuffers::DetachedBuffer> & batch_data, size_t bytes_to_write) const
{
    return append<flatbuffers::DetachedBuffer>(batch_data, bytes_to_write);
}

namespace
{
/// For unit testing since we don't have good system call mockup yet
[[maybe_unused]] int hooked_writev(int, const struct iovec * iovec_start, int n)
{
    bool one_by_one = false;
    if (one_by_one)
    {
        return 1;
    }
    else
    {
        int total = 0;
        for (int i = 0; i < n; ++i)
        {
            total += iovec_start[i].iov_len;
            if (total > 7)
                return 7;
        }
        return total;
    }
}
}

template <typename ByteRange>
CallResult<size_t> AppendOnlyFile::append(const std::vector<ByteRange> & batch_data, size_t total_bytes) const
{
    assert(!read_only);

    std::vector<struct iovec> iovec(batch_data.size());
    for (size_t i = 0, n = batch_data.size(); i < n; ++i)
    {
        iovec[i].iov_base = const_cast<char *>(reinterpret_cast<const char *>(batch_data[i].data()));
        iovec[i].iov_len = batch_data[i].size();
    }

    size_t written_bytes = 0;
    struct iovec * iovec_start = iovec.data();
    size_t num_vec = iovec.size();

    auto handle_partial_write = [&]() -> std::pair<struct iovec *, size_t>
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::LocalFileSystemShortWrite};

        /// Handle short writes although it is very unlikely but still possible because of signal etc
        /// Calculate the start position for the remaining data to write
        size_t accumulated_bytes = 0;
        for (size_t index = 0; const auto & data : batch_data)
        {
            accumulated_bytes += data.size();
            if (accumulated_bytes == written_bytes)
            {
                /// Completely write the batch_data[0, index] range
                return {&iovec[index + 1], batch_data.size() - (index + 1)};
            }
            else if (accumulated_bytes > written_bytes)
            {
                auto remaining = accumulated_bytes - written_bytes;
                /// Partially write the batch_data[index]
                iovec_start = &iovec[index];
                iovec_start->iov_base = const_cast<char *>(reinterpret_cast<const char *>(data.data())) + (data.size() - remaining);
                iovec_start->iov_len = remaining;
                return {iovec_start, batch_data.size() - index};
            }

            ++index;
        }

        UNREACHABLE();
    };

    while (true)
    {
        assert(num_vec > 0);
        if (auto n = ::writev(fd, iovec_start, static_cast<int>(num_vec)); n >= 0)
        /// if (auto n = hooked_writev(fd, iovec_start, num_vec); n >= 0)
        {
            written_bytes += n;

            assert(written_bytes <= total_bytes);

            if (likely(written_bytes == total_bytes))
                break;

            std::tie(iovec_start, num_vec) = handle_partial_write();
        }
        else
        {
            if (errno != EINTR)
                /// FIXME, map errno to DB::ErrorCodes
                return CallResult<size_t>{written_bytes, errno};

            std::tie(iovec_start, num_vec) = handle_partial_write();
        }
    }

    assert(written_bytes <= total_bytes);
    return CallResult<size_t>(written_bytes, /*error_code_=*/0);
}

CallResult<size_t> AppendOnlyFile::append(const char * data, uint64_t bytes_to_write) const
{
    assert(!read_only);

    size_t written_bytes = 0;
    while (bytes_to_write)
    {
        /// FIXME: io_uring
        if (auto n = ::write(fd, data + written_bytes, bytes_to_write); n >= 0)
        {
            written_bytes += n;
            bytes_to_write -= n;
        }
        else
        {
            if (errno != EINTR)
                /// FIXME, map errno to DB::ErrorCodes
                return CallResult<size_t>{written_bytes, errno};
        }
    }

    return CallResult<size_t>{written_bytes, /*error_code_=*/0};
}

CallResult<size_t> AppendOnlyFile::append(std::span<char> data) const
{
    return append(data.data(), data.size());
}

/// Keep reading until expected bytes have been read or EOF is reached
CallResult<uint64_t> AppendOnlyFile::read(char * dst, uint64_t bytes_to_read, uint64_t offset) const
{
    uint64_t read_bytes = 0;
    while (bytes_to_read)
    {
        auto n = ::pread(fd, dst + read_bytes, bytes_to_read, offset + read_bytes);
        if (n > 0)
        {
            read_bytes += n;
            bytes_to_read -= n;
        }
        else if (n == 0)
        {
            /// EOF
            break;
        }
        else
        {
            if (errno != EINTR)
                return CallResult<uint64_t>{0, errno};
        }
    }

    return CallResult{read_bytes, /*error_code_=*/0};
}

CallResult<uint64_t> AppendOnlyFile::read(std::span<char> dst, uint64_t offset) const
{
    return read(dst.data(), dst.size(), offset);
}

int64_t AppendOnlyFile::zeroCopyTo(
    uint64_t offset, uint64_t count, int32_t target_fd) const /// NOLINT(readability-convert-member-functions-to-static)
{
    (void)offset;
    (void)count;
    (void)target_fd;
    return 0;
}

int64_t AppendOnlyFile::zeroCopyFrom(
    int32_t source_fd, uint64_t offset, uint64_t count) const /// NOLINT(readability-convert-member-functions-to-static)
{
    (void)source_fd;
    (void)offset;
    (void)count;
    return 0;
}

CallResult<uint64_t> AppendOnlyFile::size() const
{
    struct stat buf;
    if (auto res = fstat(fd, &buf); res < 0)
        return CallResult<uint64_t>{0, errno};

    return CallResult<uint64_t>(buf.st_size, 0);
}

CallResult<int64_t> AppendOnlyFile::lastModified() const
{
    struct stat buf;
    if (auto res = fstat(fd, &buf); res < 0)
        return CallResult<int64_t>{0, errno};

#if defined(OS_DARWIN)
    return CallResult<int64_t>{buf.st_mtimespec.tv_sec * 1000 + buf.st_mtimespec.tv_nsec / 1000000, 0};
#else
    return CallResult<int64_t>{buf.st_mtim.tv_sec * 1000 + buf.st_mtim.tv_nsec / 1000000, 0};
#endif
}

int AppendOnlyFile::truncate(uint64_t length) const
{
    assert(!read_only);
    return ftruncate(fd, length) == 0 ? 0 : errno;
}

CallResult<uint64_t> AppendOnlyFile::sendTo(uint64_t start_offset, uint64_t count, DB::WriteBuffer & wb) const
{
    std::vector<char> buf(4096, '0');

    uint64_t remaining = count;
    while (remaining != 0)
    {
        auto to_read = remaining > buf.size() ? buf.size() : remaining;
        [[maybe_unused]] auto r = read(buf.data(), to_read, start_offset + count - remaining);
        if (r.hasError())
            return CallResult{count - remaining, r.error_code};

        assert(static_cast<uint64_t>(r.result) == to_read);
        wb.write(buf.data(), to_read);
        remaining -= to_read;
    }

    return CallResult{count, /*error_code_=*/0};
}
}
