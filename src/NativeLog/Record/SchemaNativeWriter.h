#pragma once

#include <vector>

namespace DB
{
class WriteBuffer;
class Block;
}

namespace nlog
{
/// Serializes the stream of blocks in their native binary format according to table schema version
class SchemaNativeWriter final
{
public:
    SchemaNativeWriter(DB::WriteBuffer & ostr_, const std::vector<uint16_t> & column_positions_);

    void write(const DB::Block & block);

private:
    DB::WriteBuffer & ostr;
    const std::vector<uint16_t> & column_positions;
};

}
