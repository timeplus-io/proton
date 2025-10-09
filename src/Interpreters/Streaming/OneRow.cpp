#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/OneRow.h>

namespace DB
{

void OneRow::serialize(WriteBuffer & wb) const
{
    writeVarUInt(row.size(), wb);

    for (const auto & field : row)
        writeFieldBinary(field, wb);
}

void OneRow::deserialize(ReadBuffer & rb)
{
    uint64_t size = 0;
    readVarUInt(size, rb);
    row.reserve(size);

    for (size_t i = 0; i < size; ++i)
        row.emplace_back(readFieldBinary(rb));
}

void OneRow::update(Block & block, size_t row_num)
{
    return update(block, row_num, row);
}

void OneRow::update(Block & block, size_t row_num, Row & target_row)
{
    assert(block.columns() == target_row.size());

    for (size_t pos = 0; const auto & col : block)
        col.column->get(row_num, target_row[pos++]);
}

}
