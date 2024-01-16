#pragma once

#include <base/defines.h>
#include <fmt/format.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

namespace Streaming
{
struct CachedBlockMetrics
{
    size_t total_rows = 0;
    size_t total_blocks = 0;
    size_t total_metadata_bytes = 0;
    size_t total_data_bytes = 0;
    size_t gced_blocks = 0;

    ALWAYS_INLINE size_t totalBytes() const { return total_metadata_bytes + total_data_bytes; }

    std::string string() const
    {
        return fmt::format(
            "total_rows={} total_bytes={} total_blocks={} gced_blocks={} (total_metadata_bytes:{} total_data_bytes:{})",
            total_rows,
            totalBytes(),
            total_blocks,
            gced_blocks,
            total_metadata_bytes,
            total_data_bytes);
    }

    /// [Legacy]
    static constexpr VersionType HAS_STATE_VERSION = 1;
    void deserialize(ReadBuffer & rb, VersionType version);
};
}
}
