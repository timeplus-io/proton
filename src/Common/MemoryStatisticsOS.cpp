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

MemoryStatisticsOS::MemoryStatisticsOS()
{
    fd = ::open(filename, O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
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

    return data;
}

}

#endif
