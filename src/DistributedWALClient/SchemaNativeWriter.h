#pragma once

#include <base/types.h>

namespace DB
{
class WriteBuffer;
class Block;
}

namespace DWAL
{
/// Serializes the stream of blocks in their native binary format according to table schema version
class SchemaNativeWriter final
{
public:
    SchemaNativeWriter(DB::WriteBuffer & ostr_, uint16_t schema_version_);

    void write(const DB::Block & block);
    void flush();

private:
    DB::WriteBuffer & ostr;
    uint16_t schema_version;
};

}
