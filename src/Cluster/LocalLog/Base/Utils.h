#pragma once

#include <fmt/format.h>
#include <Poco/UUID.h>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace cluster::nlog
{
/// `flushFile` flushes dirty directory or file. If include_meta is true, metadata will be flushed along with data
/// Otherwise only data is flushed which reduces I/O
/// \return error code from OS
int flushFile(const fs::path & file, bool include_meta = false);

/// \return error code from OS
int flushFile(int32_t fd, bool include_meta = false);

/// \return error code from OS
int flushFileTail(int32_t fd, uint64_t offset);

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
