#pragma once

#include "SchemaProvider.h"

#include <Core/Block.h>

namespace DB
{
class ReadBuffer;
}

namespace DWAL
{
/// Serializes the stream of blocks in their native binary format according to table schema version
class SchemaNativeReader final
{
public:
    SchemaNativeReader(
        DB::ReadBuffer & istr_, uint16_t & schema_version_, bool partial_, const SchemaContext & schema_ctx_);

    DB::Block read();

private:
    inline DB::Block readFullForRequestFull(uint32_t rows, DB::Block res);
    inline DB::Block readFullForRequestPartial(uint32_t rows, const DB::Block & header);
    inline DB::Block readPartialForRequestFull(uint16_t columns, uint32_t rows, DB::Block res);
    inline DB::Block readPartialForRequestPartial(uint16_t columns, uint32_t rows, const DB::Block & header);
    inline DB::Block sortColumnOrder(DB::Block res);

private:
    DB::ReadBuffer & istr;
    uint16_t & schema_version;
    bool partial;
    const SchemaContext & schema_ctx;
};

}
