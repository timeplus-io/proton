#pragma once

#include <string>

#include <Common/Exception.h>

#include <fmt/format.h>
#include <Poco/UUID.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace nlog
{
/// `replaceSuffix` validates the incoming `s` ends with `suffix`, if the validation failed, it throws
/// otherwise it will replace the suffix with the `new_suffix`
std::string & replaceSuffix(std::string & s, const std::string & suffix, const std::string & new_suffix);

/// `flushFile` flushes dirty directory or file. If include_meta is true, metadata will be flushed along with data
/// Otherwise only data is flushed which reduces I/O
/// Throw exception if flush fails
void flushFile(const fs::path & file, bool include_meta = false);

void flushFile(int32_t fd, bool include_meta = false);

inline bool isReadable(fs::perms perms)
{
    auto read_mask = fs::perms::owner_read | fs::perms::group_read | fs::perms::others_read;
    return (perms & read_mask) != fs::perms::none;
}

inline bool isWriteable(fs::perms perms)
{
    auto write_mask = fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write;
    return (perms & write_mask) != fs::perms::none;
}

inline bool isReadWritable(fs::perms perms)
{
    return isReadable(perms) && isWriteable(perms);
}

inline bool isReadableOrWritable(fs::perms perms)
{
    return isReadable(perms) || isWriteable(perms);
}

std::string uuidStringWithoutHyphen();
Poco::UUID uuid();

}
