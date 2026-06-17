#if defined(OS_LINUX)

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include "MemoryStatisticsOS.h"

#include <Common/logger_useful.h>
#include <base/getPageSize.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
}

namespace
{
uint64_t extractMemoryValue(ReadBuffer & in)
{
    uint64_t value = 0;
    skipWhitespaceIfAny(in, true);

    readIntText(value, in);

    skipWhitespaceIfAny(in, true);
    assertString("kB", in);

    return value * 1024;
}
}

static constexpr auto filename = "/proc/self/status";
static constexpr auto rollup_filename = "/proc/self/smaps_rollup";

MemoryStatisticsOS::MemoryStatisticsOS()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);

    /// smaps_rollup was added in Linux 4.14. Absence is not an error — the
    /// caller just gets zero-valued decomposition fields.
    rollup_fd = ::open(rollup_filename, O_RDONLY | O_CLOEXEC);
}

MemoryStatisticsOS::~MemoryStatisticsOS()
{
    if (0 != ::close(fd))
    {
        try
        {
            throwFromErrno(
                    "File descriptor for \"" + std::string(filename) + "\" could not be closed. "
                    "Something seems to have gone wrong. Inspect errno.", ErrorCodes::CANNOT_CLOSE_FILE);
        }
        catch (const ErrnoException &)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (-1 != rollup_fd)
        ::close(rollup_fd);
}

MemoryStatisticsOS::Data MemoryStatisticsOS::get() const
{
    Data data;

    constexpr size_t buf_size = 1024;
    char buf[buf_size];

    ssize_t res = 0;

    do
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno("Cannot read from file " + std::string(filename), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        assert(res >= 0);
        break;
    } while (true);

    ReadBufferFromMemory in(buf, res);

    uint64_t data_size = 0;
    uint64_t stack_size = 0;
    while (!in.eof())
    {
        String key;
        readString(key, in);

        if (key == "VmSize:")
            data.virt = extractMemoryValue(in);
        else if (key == "VmRSS:")
            data.resident = extractMemoryValue(in);
        else if (key == "RssShmem:")
            data.shared = extractMemoryValue(in);
        else if (key == "VmExe:")
            data.code = extractMemoryValue(in);
        else if (key == "VmData:")
            data_size = extractMemoryValue(in);
        else if (key == "VmStk:")
            stack_size = extractMemoryValue(in);

        skipToNextLineOrEOF(in);
    }
    data.data_and_stack = data_size + stack_size;

    if (-1 != rollup_fd)
    {
        /// smaps_rollup is small (< 1 KiB in practice) and the kernel
        /// aggregates it into a single virtual mapping, so one pread is enough.
        ssize_t rollup_res = 0;
        do
        {
            rollup_res = ::pread(rollup_fd, buf, buf_size, 0);
            if (-1 == rollup_res)
            {
                if (errno == EINTR)
                    continue;
                /// Soft-fail: leave decomposition fields at zero. Do not throw
                /// — smaps_rollup may become unreadable under seccomp / hidepid.
                rollup_res = 0;
            }
            break;
        } while (true);

        if (rollup_res > 0)
        {
            data.smaps_rollup_available = true;
            ReadBufferFromMemory rollup_in(buf, rollup_res);
            /// First line is a mapping header like `555...-7ff... ---p ... [rollup]`.
            /// Skip it; all subsequent lines are `Key:<spaces>N kB`.
            skipToNextLineOrEOF(rollup_in);

            while (!rollup_in.eof())
            {
                String key;
                /// smaps_rollup separates key from value with spaces (unlike
                /// /proc/self/status which uses tabs), so stop at the first
                /// space rather than using readString's `\t|\n` stop chars.
                readStringUntilWhitespace(key, rollup_in);

                if (key == "Pss:")
                    data.pss = extractMemoryValue(rollup_in);
                else if (key == "Private_Dirty:")
                    data.private_dirty = extractMemoryValue(rollup_in);
                else if (key == "Anonymous:")
                    data.anonymous = extractMemoryValue(rollup_in);
                else if (key == "AnonHugePages:")
                    data.anon_huge_pages = extractMemoryValue(rollup_in);
                else if (key == "Swap:")
                    data.swap = extractMemoryValue(rollup_in);

                skipToNextLineOrEOF(rollup_in);
            }
        }
    }

    return data;
}

}

#endif
