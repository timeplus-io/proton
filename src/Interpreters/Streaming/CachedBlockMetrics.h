#pragma once

#include <fmt/format.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

namespace Streaming
{
struct CachedBlockMetrics
{
    size_t current_total_blocks = 0;
    size_t current_total_bytes = 0;
    size_t total_blocks = 0;
    size_t total_bytes = 0;
    size_t gced_blocks = 0;

    std::string string() const
    {
        return fmt::format(
            "total_bytes={} total_blocks={} current_total_bytes={} current_total_blocks={} gced_blocks={}",
            total_bytes,
            total_blocks,
            current_total_bytes,
            current_total_blocks,
            gced_blocks);
    }

    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);
};
}
}
