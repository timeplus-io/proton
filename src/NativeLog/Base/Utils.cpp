#include "Utils.h"

#include <Common/Exception.h>

#include <Poco/UUIDGenerator.h>

#include <fcntl.h>
#include <unistd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FLUSH_FILE;
    extern const int INVALID_DATA;
}
}

namespace nlog
{
namespace
{
    static Poco::UUIDGenerator generator;
}

std::string & replaceSuffix(std::string & s, const std::string & suffix, const std::string & new_suffix)
{
    if (s.ends_with(suffix))
        throw DB::Exception(fmt::format("Expected string to end with '{}', but string is '{}'", suffix, s), DB::ErrorCodes::INVALID_DATA);

    return s.replace(s.size() - suffix.size(), suffix.size(), new_suffix);
}

void flushFile(const fs::path & file, bool include_meta)
{
    auto fd = ::open(file.c_str(), O_RDONLY);
    if (fd < 0)
        DB::throwFromErrnoWithPath(fmt::format("Cannot flush file {}", file.c_str()), file.string(), DB::ErrorCodes::CANNOT_OPEN_FILE);

    int res = 0;
    if (include_meta)
        res = ::fsync(fd);
    else
        res = ::fdatasync(fd);

    if (res < 0)
        DB::throwFromErrnoWithPath(fmt::format("Cannot flush file {}", file.c_str()), file.string(), DB::ErrorCodes::CANNOT_FLUSH_FILE);
}

void flushFile(int32_t fd, bool include_meta)
{
    int res = 0;
    if (include_meta)
        res = ::fsync(fd);
    else
        res = ::fdatasync(fd);

    if (res < 0)
        throw DB::Exception(DB::ErrorCodes::CANNOT_FLUSH_FILE, fmt::format("Cannot flush file descriptor {}", fd));
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
