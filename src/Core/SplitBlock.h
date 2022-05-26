#pragma once

#include <Core/Block.h>

namespace DB
{
struct BlockRangeSplitterInf
{
    virtual std::vector<std::pair<UInt64, Block>> split(Block block) const = 0;
    virtual ~BlockRangeSplitterInf() = default;
};

using BlockRangeSplitterPtr = std::unique_ptr<BlockRangeSplitterInf>;

/// `splitBlockRange` splits source block according to range. The column used to split the block must be
/// integral type, (u)int8/16/32/64 or datetime / datetime64
BlockRangeSplitterPtr
createBlockRangeSplitter(TypeIndex type_index, size_t split_column_pos, UInt64 range, bool calc_min_max);
}
