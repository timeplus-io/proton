#pragma once

#include <Core/Block.h>
#include <Processors/Chunk.h>

namespace DB
{

/// LightChunk is designed to have columns only and is used to cache in memory without too much overhead
/// especially in stream processing scenarios
struct LightChunk
{
    Columns data;

    LightChunk() = default;
    LightChunk(Columns && data_) : data(std::move(data_)) { }
    LightChunk(Chunk && chunk) : data(chunk.detachColumns()) { }
    LightChunk(const Block & block) : data(block.getColumns()) { }

    size_t rows() const { return data.empty() ? 0 : data[0]->size(); }
    size_t columns() const { return data.size(); }

    const Columns & getColumns() const { return data; }
    Columns detachColumns() { return std::move(data); }

    UInt64 allocatedBytes() const
    {
        UInt64 res = 0;
        for (const auto & column : data)
            res += column->allocatedBytes();

        return res;
    }

    void clear() noexcept { data.clear(); }

    operator bool() const noexcept { return !data.empty(); }

    /// Dummy interface to make RefCountBlockList happy
    Int64 minTimestamp() const { return 0; }
    Int64 maxTimestamp() const { return 0; }
};

struct LightChunkWithTimestamp : public LightChunk
{
    Int64 min_timestamp = 0;
    Int64 max_timestamp = 0;

    LightChunkWithTimestamp() = default;
    LightChunkWithTimestamp(Columns && data_) : LightChunk(std::move(data_)) { }
    LightChunkWithTimestamp(const Block & block)
        : LightChunk(block), min_timestamp(block.minTimestamp()), max_timestamp(block.maxTimestamp())
    {
    }

    void setMinTimestamp(Int64 min_ts_) noexcept { min_timestamp = min_ts_; }
    void setMaxTimestamp(Int64 max_ts_) noexcept { max_timestamp = max_ts_; }
    Int64 minTimestamp() const noexcept { return min_timestamp; }
    Int64 maxTimestamp() const noexcept { return max_timestamp; }
};

}