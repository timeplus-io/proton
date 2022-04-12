#pragma once

#include "SchemaProvider.h"

#include <Core/Block.h>

namespace DB
{
class ReadBuffer;
}

namespace nlog
{
/// Serializes the stream of blocks in their native binary format according to table schema version
class SchemaNativeReader final
{
public:
    SchemaNativeReader(
        DB::ReadBuffer & istr_, bool partial_, uint16_t schema_version_, const SchemaContext & schema_ctx_);

    void read(DB::Block & res);

private:
    inline void readFullForRequestFull(uint32_t rows, const DB::Block & header, DB::Block & res);
    inline void readFullForRequestPartial(uint32_t rows, const DB::Block & header, DB::Block & res);
    inline void readPartialForRequestFull(uint16_t columns, uint32_t rows, const DB::Block & header, DB::Block & res);
    inline void readPartialForRequestPartial(uint16_t columns, uint32_t rows, const DB::Block & header, DB::Block & res);
    inline void sortColumnOrder(DB::Block & res);

private:
    DB::ReadBuffer & istr;
    bool partial;
    uint16_t schema_version;
    const SchemaContext & schema_ctx;
};

}
