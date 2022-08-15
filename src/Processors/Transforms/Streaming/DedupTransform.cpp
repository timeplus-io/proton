#include "DedupTransform.h"

#include <Columns/ColumnsNumber.h>

namespace DB
{
namespace Streaming
{
DedupTransform::DedupTransform(const Block & input_header, const Block & output_header, FunctionDescriptionPtr dedup_func_desc_)
    : ISimpleTransform(input_header, output_header, false)
    , dedup_func_desc(std::move(dedup_func_desc_))
    , chunk_header(output_header.getColumns(), 0)
{
    assert(dedup_func_desc);

    calculateColumns(input_header, dedup_func_desc->input_columns);
}

void DedupTransform::transform(Chunk & chunk)
{
    if (!chunk.hasRows())
    {
        /// The downstream header is different than the output of this transform
        /// We need use the current output header
        chunk.setColumns(chunk_header.cloneEmptyColumns(), 0);
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    Block expr_block;

    /// Most of the time, we copied only one column
    for (auto pos : expr_column_positions)
        expr_block.insert(block.getByPosition(pos));

    dedup_func_desc->expr->execute(expr_block);

    assert(expr_block);

    auto * filter_column = checkAndGetColumn<ColumnBool>(expr_block.getByPosition(0).column.get());
    assert(filter_column);

    auto rows = block.rows();
    const auto & filter = filter_column->getData();

    for (auto & col_with_name_type : block)
        col_with_name_type.column = col_with_name_type.column->filter(filter, rows);

    /// FIXME, drop the unneeded columns here
    chunk.setColumns(block.getColumns(), block.rows());
}

void DedupTransform::calculateColumns(const Block & input_header, const Names & input_columns)
{
    expr_column_positions.reserve(input_columns.size());

    /// Calculate the positions of dependent columns in input chunk
    for (const auto & col_name : input_columns)
        expr_column_positions.push_back(input_header.getPositionByName(col_name));
}
}
}
