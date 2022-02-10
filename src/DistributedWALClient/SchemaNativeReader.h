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
    SchemaNativeReader(DB::ReadBuffer & istr_, uint16_t & schema_version_, const SchemaProvider & schema_);

    DB::Block read();

private:
    DB::ReadBuffer & istr;
    uint16_t & schema_version;
    const SchemaProvider & schema;
};

}
