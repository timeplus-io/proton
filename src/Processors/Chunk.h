#pragma once

#include <Columns/IColumn.h>

/// proton : starts
#include <Core/Streaming/SubstreamID.h>
#include <Core/Streaming/Watermark.h>
#include <Checkpoint/CheckpointContextFwd.h>
/// proton : ends

#include <unordered_map>

namespace DB
{

/// proton : starts
struct ChunkContext : public COW<ChunkContext>
{
private:
    friend class COW<ChunkContext>;

    /// This is internal method to use from COW.
    /// It performs shallow copy with copy-ctor and not useful from outside.
    /// If you want to copy column for modification, look at 'mutate' method.
    [[nodiscard]] MutablePtr clone() const { return ChunkContext::create(*this); }

public:
    static constexpr UInt64 WATERMARK_FLAG = 0x1;
    static constexpr UInt64 APPEND_TIME_FLAG = 0x2;
    static constexpr UInt64 HISTORICAL_DATA_START_FLAG = 0x4;
    static constexpr UInt64 HISTORICAL_DATA_END_FLAG = 0x8;
    static constexpr UInt64 CONSECUTIVE_DATA_FLAG = 0x10;
    static constexpr UInt64 AVOID_WATERMARK_FLAG = 0x8000'0000'0000'0000;

    /// A pair of Int64, flags represent what they mean
    Streaming::SubstreamID id = Streaming::INVALID_SUBSTREAM_ID;
    Int64 ts_1 = 0;
    UInt64 flags = 0;
    CheckpointContextPtr ckpt_ctx;

    ALWAYS_INLINE Int64 isHistoricalDataStart() const { return flags & HISTORICAL_DATA_START_FLAG; }
    ALWAYS_INLINE Int64 isHistoricalDataEnd() const { return flags & HISTORICAL_DATA_END_FLAG; }

    ALWAYS_INLINE void setMark(UInt64 mark) { flags |= mark; }

    ALWAYS_INLINE explicit operator bool () const { return flags != 0 || id != Streaming::INVALID_SUBSTREAM_ID || ckpt_ctx != nullptr; }

    ALWAYS_INLINE bool hasWatermark() const { return flags & WATERMARK_FLAG; }

    ALWAYS_INLINE bool hasTimeoutWatermark() const { return hasWatermark() && getWatermark() == Streaming::TIMEOUT_WATERMARK; }

    ALWAYS_INLINE void setSubstreamID(Streaming::SubstreamID id_) { id = std::move(id_); }

    ALWAYS_INLINE void setWatermark(Int64 watermark)
    {
        assert(watermark != Streaming::INVALID_WATERMARK);
        flags |= WATERMARK_FLAG;
        ts_1 = watermark;
    }

    ALWAYS_INLINE Int64 getWatermark() const
    {
        assert(hasWatermark());

        return ts_1;
    }

    ALWAYS_INLINE void clearWatermark()
    {
        if (hasWatermark())
        {
            flags &= ~WATERMARK_FLAG;
            ts_1 = Streaming::INVALID_WATERMARK;
        }
    }

    ALWAYS_INLINE void setConsecutiveDataFlag()
    {
        flags |= CONSECUTIVE_DATA_FLAG;
        setAvoidWatermark();
    }

    ALWAYS_INLINE bool isConsecutiveData() const { return flags & CONSECUTIVE_DATA_FLAG; }

    ALWAYS_INLINE void setAvoidWatermark() { flags |= AVOID_WATERMARK_FLAG; }

    ALWAYS_INLINE bool avoidWatermark() const { return ckpt_ctx || (flags & AVOID_WATERMARK_FLAG); }

    ALWAYS_INLINE bool hasAppendTime() const { return flags & APPEND_TIME_FLAG; }
    ALWAYS_INLINE void setAppendTime(Int64 append_time)
    {
        if (append_time > 0)
        {
            flags |= APPEND_TIME_FLAG;
            ts_1 = append_time;
        }
    }

    ALWAYS_INLINE Int64 getAppendTime() const { assert(hasAppendTime()); return ts_1; }

    void setCheckpointContext(CheckpointContextPtr ckpt_ctx_) { ckpt_ctx = std::move(ckpt_ctx_); }

    CheckpointContextPtr getCheckpointContext() const { return ckpt_ctx; }

    const Streaming::SubstreamID & getSubstreamID() const { return id; }
};
using ChunkContextPtr = ChunkContext::Ptr;
using MutableChunkContextPtr = ChunkContext::MutablePtr;
/// proton : ends

class ChunkInfo
{
public:
    virtual ~ChunkInfo() = default;
    ChunkInfo() = default;
};

using ChunkInfoPtr = std::shared_ptr<const ChunkInfo>;

/**
 * Chunk is a list of columns with the same length.
 * Chunk stores the number of rows in a separate field and supports invariant of equal column length.
 *
 * Chunk has move-only semantic. It's more lightweight than block cause doesn't store names, types and index_by_name.
 *
 * Chunk can have empty set of columns but non-zero number of rows. It helps when only the number of rows is needed.
 * Chunk can have columns with zero number of rows. It may happen, for example, if all rows were filtered.
 * Chunk is empty only if it has zero rows and empty list of columns.
 *
 * Any ChunkInfo may be attached to chunk.
 * It may be useful if additional info per chunk is needed. For example, bucket number for aggregated data.
**/

class Chunk
{
public:
    Chunk() = default;
    Chunk(const Chunk & other) = delete;
    Chunk(Chunk && other) noexcept
        : columns(std::move(other.columns))
        , num_rows(other.num_rows)
        , chunk_info(std::move(other.chunk_info))
        , chunk_ctx(std::move(other.chunk_ctx))
    {
        other.num_rows = 0;
    }

    Chunk(Columns columns_, UInt64 num_rows_);
    Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_, ChunkContextPtr chunk_ctx_);
    Chunk(MutableColumns columns_, UInt64 num_rows_);
    Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_, ChunkContextPtr chunk_ctx);

    Chunk & operator=(const Chunk & other) = delete;
    Chunk & operator=(Chunk && other) noexcept
    {
        columns = std::move(other.columns);
        chunk_info = std::move(other.chunk_info);
        chunk_ctx = std::move(other.chunk_ctx);
        num_rows = other.num_rows;
        other.num_rows = 0;
        return *this;
    }

    Chunk clone() const;

    void swap(Chunk & other)
    {
        columns.swap(other.columns);
        chunk_info.swap(other.chunk_info);
        chunk_ctx.swap(other.chunk_ctx);
        std::swap(num_rows, other.num_rows);
    }

    void clear()
    {
        num_rows = 0;
        columns.clear();
        chunk_info.reset();
        chunk_ctx.reset();
    }

    const Columns & getColumns() const { return columns; }
    void setColumns(Columns columns_, UInt64 num_rows_);
    void setColumns(MutableColumns columns_, UInt64 num_rows_);
    Columns detachColumns();
    MutableColumns mutateColumns();
    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    const ChunkInfoPtr & getChunkInfo() const { return chunk_info; }
    bool hasChunkInfo() const { return chunk_info != nullptr; }
    void setChunkInfo(ChunkInfoPtr chunk_info_) { chunk_info = std::move(chunk_info_); }

    UInt64 rows() const { return num_rows; }
    UInt64 getNumRows() const { return num_rows; }
    UInt64 getNumColumns() const { return columns.size(); }
    bool hasRows() const { return num_rows > 0; }
    bool hasColumns() const { return !columns.empty(); }
    bool empty() const { return !hasRows() && !hasColumns(); }
    operator bool() const { return !empty(); }

    void addColumn(ColumnPtr column);
    void addColumn(size_t position, ColumnPtr column);
    void erase(size_t position);

    UInt64 bytes() const;
    UInt64 allocatedBytes() const;
    UInt64 allocatedDataBytes() const;
    UInt64 allocatedMetadataBytes() const;

    std::string dumpStructure() const;

    void append(const Chunk & chunk);
    void append(Chunk && chunk);

    /// proton : starts
    bool hasWatermark() const { return chunk_ctx && chunk_ctx->hasWatermark(); }

    bool hasTimeoutWatermark() const { return chunk_ctx && chunk_ctx->hasTimeoutWatermark(); }

    bool hasChunkContext() const { return chunk_ctx && chunk_ctx->operator bool(); }

    bool requestCheckpoint() const { return chunk_ctx && chunk_ctx->getCheckpointContext(); }

    void setChunkContext(ChunkContextPtr chunk_ctx_) { chunk_ctx = std::move(chunk_ctx_); }

    ChunkContextPtr getChunkContext() const { return chunk_ctx; }

    ChunkContextPtr getOrCreateChunkContext()
    {
        if (chunk_ctx)
            return chunk_ctx;

        setChunkContext(ChunkContext::create());

        return chunk_ctx;
    }

    CheckpointContextPtr getCheckpointContext() const
    {
        if (chunk_ctx)
            return chunk_ctx->getCheckpointContext();

        return nullptr;
    }

    const Streaming::SubstreamID & getSubstreamID() const
    {
        if (chunk_ctx)
            return chunk_ctx->getSubstreamID();

        return Streaming::INVALID_SUBSTREAM_ID;
    }

    void trySetSubstreamID(Streaming::SubstreamID id)
    {
        if (chunk_ctx)
        {
            auto mutate_chunk_ctx = ChunkContext::mutate(chunk_ctx);
            mutate_chunk_ctx->setSubstreamID(std::move(id));
            chunk_ctx = std::move(mutate_chunk_ctx);
        }
    }

    Int64 getWatermark() const
    {
        assert(chunk_ctx);
        return chunk_ctx->getWatermark();
    }

    void setWatermark(Int64 watermark)
    {
        auto mutate_chunk_ctx = chunk_ctx ? ChunkContext::mutate(chunk_ctx) : ChunkContext::create();
        mutate_chunk_ctx->setWatermark(watermark);
        chunk_ctx = std::move(mutate_chunk_ctx);
    }

    void reserve(size_t num_columns)
    {
        columns.reserve(num_columns);
    }

    bool isConsecutiveData() const
    {
        return chunk_ctx && chunk_ctx->isConsecutiveData();
    }

    bool avoidWatermark() const
    {
        return chunk_ctx && chunk_ctx->avoidWatermark();
    }

    void clearWatermark()
    {
        if (chunk_ctx && chunk_ctx->hasWatermark())
        {
            auto mutate_chunk_ctx = ChunkContext::mutate(chunk_ctx);
            mutate_chunk_ctx->clearWatermark();
            chunk_ctx = std::move(mutate_chunk_ctx);
        }
    }

    void clearRequestCheckpoint()
    {
        if (chunk_ctx && chunk_ctx->getCheckpointContext())
        {
            auto mutate_chunk_ctx = ChunkContext::mutate(chunk_ctx);
            mutate_chunk_ctx->setCheckpointContext(nullptr);
            chunk_ctx = std::move(mutate_chunk_ctx);
        }
    }

    void setCheckpointContext(CheckpointContextPtr ckpt_ctx)
    {
        auto mutate_chunk_ctx = chunk_ctx ? ChunkContext::mutate(chunk_ctx) : ChunkContext::create();
        mutate_chunk_ctx->setCheckpointContext(ckpt_ctx);
        chunk_ctx = std::move(mutate_chunk_ctx);
    }

    void setConsecutiveDataFlag()
    {
        auto mutate_chunk_ctx = chunk_ctx ? ChunkContext::mutate(chunk_ctx) : ChunkContext::create();
        mutate_chunk_ctx->setConsecutiveDataFlag();
        chunk_ctx = std::move(mutate_chunk_ctx);
    }

    bool isHistoricalDataStart() const { return chunk_ctx && chunk_ctx->isHistoricalDataStart(); }
    bool isHistoricalDataEnd() const { return chunk_ctx && chunk_ctx->isHistoricalDataEnd(); }

    /// Dummy interface to make RefCountBlockList happy
    Int64 minTimestamp() const { return 0; }
    Int64 maxTimestamp() const { return 0;}
    /// proton : ends

private:
    Columns columns;
    UInt64 num_rows = 0;
    ChunkInfoPtr chunk_info;
    /// COW<ChunkContext>::Ptr, it can be shared by multiple processors (only copy on write)
    ChunkContextPtr chunk_ctx;

    void checkNumRowsIsConsistent();
};

using Chunks = std::vector<Chunk>;
using ChunkList = std::list<Chunk>;

/// Extension to support delayed defaults. AddingDefaultsProcessor uses it to replace missing values with column defaults.
class ChunkMissingValues : public ChunkInfo
{
public:
    using RowsBitMask = std::vector<bool>; /// a bit per row for a column

    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    void setBit(size_t column_idx, size_t row_idx);
    bool empty() const { return rows_mask_by_column_id.empty(); }
    size_t size() const { return rows_mask_by_column_id.size(); }
    void clear() { rows_mask_by_column_id.clear(); }

private:
    using RowsMaskByColumnId = std::unordered_map<size_t, RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less columns and rows then related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

/// Converts all columns to full serialization in chunk.
/// It's needed, when you have to access to the internals of the column,
/// or when you need to perform operation with two columns
/// and their structure must be equal (e.g. compareAt).
void convertToFullIfSparse(Chunk & chunk);

}
