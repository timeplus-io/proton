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

    void reserve(size_t rows)
    {
        for (auto & col : data)
            col->assumeMutable()->reserve(rows);
    }

    /// Inplace concat
    void concat(const LightChunk & other)
    {
        auto added_rows = other.rows();
        if (added_rows <= 0)
            return;

        assert(columns() == other.columns());
        for (size_t c = 0; auto & col : data)
        {
            assert(col->getName() == other.data[c]->getName());
            col->assumeMutable()->insertRangeFrom(*other.data[c++], 0, added_rows);
        }
    }

    LightChunk cloneEmpty() const
    {
        LightChunk res;
        res.data.reserve(data.size());

        for (const auto & elem : data)
            res.data.emplace_back(elem->cloneEmpty());

        return res;
    }

    size_t rows() const noexcept { return data.empty() ? 0 : data[0]->size(); }
    size_t columns() const noexcept { return data.size(); }

    Columns & getColumns() noexcept { return data; }
    const Columns & getColumns() const noexcept { return data; }
    Columns detachColumns() noexcept { return std::move(data); }

    /// The column data in memory
    UInt64 byteSize() const
    {
        UInt64 res = 0;
        for (const auto & column : data)
            res += column->byteSize();
        return res;
    }

    UInt64 allocatedDataBytes() const
    {
        UInt64 res = 0;
        for (const auto & column : data)
            res += column->allocatedBytes();
        return res;
    }

    UInt64 allocatedMetadataBytes() const
    {
        UInt64 res = 0;
        for (const auto & column : data)
            res += column->allocatedMetadataBytes();

        res += sizeof(data) + data.capacity() * sizeof(ColumnPtr);
        return res;
    }

    UInt64 allocatedBytes() const { return allocatedMetadataBytes() + allocatedDataBytes(); }

    void clear() noexcept { data.clear(); }

    operator bool() const noexcept { return !data.empty(); }

    /// Dummy interface to make RefCountBlockList happy
    Int64 minTimestamp() const noexcept { return 0; }
    Int64 maxTimestamp() const noexcept { return 0; }
};

struct LightChunkWithTimestamp
{
    LightChunk chunk;
    Int64 min_timestamp = 0;
    Int64 max_timestamp = 0;

    LightChunkWithTimestamp() = default;
    LightChunkWithTimestamp(Columns && data_) : chunk(std::move(data_)) { }
    LightChunkWithTimestamp(Chunk && chunk_, Int64 min_ts, Int64 max_ts)
        : chunk(std::move(chunk_)), min_timestamp(min_ts), max_timestamp(max_ts)
    {
    }
    LightChunkWithTimestamp(const Block & block)
        : chunk(block), min_timestamp(block.minTimestamp()), max_timestamp(block.maxTimestamp()) { }

    void reserve(size_t rows) { chunk.reserve(rows); }

    /// Inplace concat
    void concat(const LightChunkWithTimestamp & other)
    {
        chunk.concat(other.chunk);
        min_timestamp = std::min(min_timestamp, other.min_timestamp);
        max_timestamp = std::max(max_timestamp, other.max_timestamp);
    }

    UInt64 byteSize() const { return chunk.byteSize(); }
    UInt64 allocatedBytes() const { return allocatedMetadataBytes() + allocatedDataBytes(); }
    UInt64 allocatedMetadataBytes() const { return chunk.allocatedMetadataBytes() + sizeof(min_timestamp) + sizeof(max_timestamp); }
    UInt64 allocatedDataBytes() const { return chunk.allocatedDataBytes(); }
    size_t rows() const noexcept { return chunk.rows(); }
    size_t columns() const noexcept { return chunk.columns(); }

    const Columns & getColumns() const noexcept { return chunk.getColumns(); }
    Columns detachColumns() noexcept { return chunk.detachColumns(); }

    void clear() noexcept { chunk.clear(); }

    operator bool() const noexcept { return chunk; }

    void setMinTimestamp(Int64 min_ts_) noexcept { min_timestamp = min_ts_; }
    void setMaxTimestamp(Int64 max_ts_) noexcept { max_timestamp = max_ts_; }
    Int64 minTimestamp() const noexcept { return min_timestamp; }
    Int64 maxTimestamp() const noexcept { return max_timestamp; }
};

}
