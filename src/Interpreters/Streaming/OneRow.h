#pragma once

#include <Core/Field.h>

namespace DB
{
class Block;

struct OneRow
{
    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

    void update(Block & block, size_t row_num);
    void resize(size_t size) { row.resize(size); }
    bool empty() const noexcept { return row.empty(); }

    static void update(Block & block, size_t row_num, Row & target_row);

    Row row;
};
}
