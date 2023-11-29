#pragma once

#include <fmt/format.h>

namespace DB
{
struct HashMapSizes
{
    size_t keys = 0;
    size_t buffer_size_in_bytes = 0;
    size_t buffer_bytes_in_cells = 0;

    std::string string() const
    {
        return fmt::format("keys={} buffer_size_in_bytes={} buffer_bytes_in_cells={}", keys, buffer_size_in_bytes, buffer_bytes_in_cells);
    }
};

}
