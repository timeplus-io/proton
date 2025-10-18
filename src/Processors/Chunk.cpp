#include <Processors/Chunk.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnSparse.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
}

Chunk::Chunk(DB::Columns columns_, UInt64 num_rows_) : columns(std::move(columns_)), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_, ChunkContextPtr chunk_ctx_)
    : columns(std::move(columns_)), num_rows(num_rows_), chunk_info(std::move(chunk_info_)), chunk_ctx(std::move(chunk_ctx_))
{
    checkNumRowsIsConsistent();
}

static Columns unmuteColumns(MutableColumns && mut_columns)
{
    Columns columns;
    columns.reserve(mut_columns.size());
    for (auto & col : mut_columns)
        columns.emplace_back(std::move(col));

    return columns;
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_)
{
    checkNumRowsIsConsistent();
}

Chunk::Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_, ChunkContextPtr chunk_ctx_)
    : columns(unmuteColumns(std::move(columns_))), num_rows(num_rows_), chunk_info(std::move(chunk_info_)), chunk_ctx(std::move(chunk_ctx_))
{
    checkNumRowsIsConsistent();
}

Chunk Chunk::clone() const
{
    return Chunk(getColumns(), getNumRows(), chunk_info, chunk_ctx);
}

void Chunk::setColumns(Columns columns_, UInt64 num_rows_)
{
    columns = std::move(columns_);
    num_rows = num_rows_;
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
    checkNumRowsIsConsistent();
}

void Chunk::setColumns(MutableColumns columns_, UInt64 num_rows_)
{
    columns = unmuteColumns(std::move(columns_));
    num_rows = num_rows_;
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
    checkNumRowsIsConsistent();
}

void Chunk::checkNumRowsIsConsistent()
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & column = columns[i];
        if (column->size() != num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk {} column {} at position {}: expected {}, got {}",
                dumpStructure(), column->getName(), i, num_rows, column->size());
    }
}

MutableColumns Chunk::mutateColumns()
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = IColumn::mutate(std::move(columns[i]));

    columns.clear();
    num_rows = 0;
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends

    return mut_columns;
}

MutableColumns Chunk::cloneEmptyColumns() const
{
    size_t num_columns = columns.size();
    MutableColumns mut_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        mut_columns[i] = columns[i]->cloneEmpty();
    return mut_columns;
}

Columns Chunk::detachColumns()
{
    num_rows = 0;
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
    return std::move(columns);
}

void Chunk::addColumn(ColumnPtr column)
{
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of rows in Chunk {} column {}: expected {}, got {}",
            dumpStructure(), column->getName(), num_rows, column->size());

    columns.emplace_back(std::move(column));
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
}

void Chunk::addColumn(size_t position, ColumnPtr column)
{
    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND,
                        "Position {} out of bound in Chunk::addColumn(), max position = {}",
                        position, columns.size() - 1);
    if (empty())
        num_rows = column->size();
    else if (column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Invalid number of rows in Chunk column {}: expected {}, got {}",
                        column->getName(), num_rows, column->size());

    columns.emplace(columns.begin() + position, std::move(column));
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
}

void Chunk::erase(size_t position)
{
    if (columns.empty())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Chunk is empty");

    if (position >= columns.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} out of bound in Chunk::erase(), max position = {}",
                        toString(position), toString(columns.size() - 1));

    columns.erase(columns.begin() + position);
    /// proton: starts
    invalidateCachedBytes();
    /// proton: ends
}

UInt64 Chunk::bytes() const
{
    /// proton: starts
    if (cached_bytes != std::numeric_limits<UInt64>::max())
    {
#ifndef NDEBUG
        UInt64 check = 0;
        for (const auto & column : columns)
            check += column->byteSize();
        chassert(check == cached_bytes);
#endif
        return cached_bytes;
    }

    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->byteSize();

    cached_bytes = res;
    return cached_bytes;
    /// proton: ends
}

UInt64 Chunk::allocatedBytes() const
{
    return allocatedMetadataBytes() + allocatedDataBytes();
}

UInt64 Chunk::allocatedDataBytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res;
}

UInt64 Chunk::allocatedMetadataBytes() const
{
    UInt64 res = 0;
    for (const auto & column : columns)
        res += column->allocatedMetadataBytes();

    res += sizeof(columns) + columns.capacity() * sizeof(ColumnPtr);
    res += sizeof(num_rows);
    res += sizeof(chunk_info);
    res += sizeof(chunk_ctx);
    return res;
}

std::string Chunk::dumpStructure() const
{
    WriteBufferFromOwnString out;
    for (const auto & column : columns)
        out << ' ' << column->dumpStructure();

    return out.str();
}

void Chunk::append(const Chunk & chunk)
{
    MutableColumns mutation = mutateColumns();
    const auto & src_cols = chunk.getColumns();
    assert(src_cols.size() == mutation.size());
    for (size_t position = 0; position < mutation.size(); ++position)
    {
        auto column = src_cols[position];
        mutation[position]->insertRangeFrom(*column, 0, column->size());
    }
    size_t rows = mutation[0]->size();
    setColumns(std::move(mutation), rows);
}

void Chunk::append(Chunk && chunk)
{
    if (!hasColumns())
        *this = std::move(chunk);
    else
        append(chunk);
}

void ChunkMissingValues::setBit(size_t column_idx, size_t row_idx)
{
    RowsBitMask & mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask[row_idx] = true;
}

const ChunkMissingValues::RowsBitMask & ChunkMissingValues::getDefaultsBitmask(size_t column_idx) const
{
    static RowsBitMask none;
    auto it = rows_mask_by_column_id.find(column_idx);
    if (it != rows_mask_by_column_id.end())
        return it->second;
    return none;
}

void convertToFullIfSparse(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = recursiveRemoveSparse(column);

    chunk.setColumns(std::move(columns), num_rows);
}

}
