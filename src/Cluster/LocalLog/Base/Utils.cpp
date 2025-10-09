#include <Cluster/LocalLog/Base/Utils.h>

#include <Poco/UUIDGenerator.h>

#include <fcntl.h>
#include <unistd.h>

namespace cluster::nlog
{
namespace
{
static Poco::UUIDGenerator generator;
}

int flushFile(const fs::path & file, bool include_meta)
{
    auto fd = ::open(file.c_str(), O_RDONLY);
    if (fd < 0)
        return errno;

    int res = 0;
    if (include_meta)
        res = ::fsync(fd);
    else
    {
#if defined(OS_DARWIN)
        res = ::fsync(fd);
#else
        res = ::fdatasync(fd);
#endif
    }

    return res == 0 ? 0 : errno;
}

int flushFile(int32_t fd, bool include_meta)
{
    int res = 0;
    if (include_meta)
        res = ::fsync(fd);
    else
    {
#if defined(OS_DARWIN)
        res = ::fsync(fd);
#else
        res = ::fdatasync(fd);
#endif
    }

    return res == 0 ? 0 : errno;
}

int flushFileTail(int32_t fd, [[maybe_unused]] uint64_t offset)
{
    int res = 0;
#if defined(OS_DARWIN)
    res = ::fsync(fd);
#else
    /// count = 0 means to the end of the file
    /// SYNC_FILE_RANGE_WAIT_AFTER : we like to block until flushed
    res = ::sync_file_range(fd, offset, /*count=*/0, SYNC_FILE_RANGE_WAIT_AFTER);
#endif

    return res == 0 ? 0 : errno;
}

std::string uuidStringWithoutHyphen()
{
    auto unique_id = generator.createRandom().toString();

    std::string s;
    s.reserve(unique_id.size() - 4);

    for (auto ch : unique_id)
        if (ch != '-')
            s.push_back(ch);

    return s;
}

Poco::UUID uuid()
{
    return generator.createRandom();
}
}
