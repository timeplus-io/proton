#pragma once

#include <Core/Block.h>

namespace DB
{
class ReadBuffer;
}

namespace cluster
{
struct SchemaContext;

/// Serializes the stream of blocks in their native binary format according to table schema version
class SchemaNativeReader final
{
    struct DeserializationContext
    {
        DB::MutableSerializationInfos infos;

        // positions in requested column_positions to read. nlog::NO_POSITION <=> skip
        std::vector<uint16_t> target_positions;
    };

public:
    SchemaNativeReader(uint16_t on_disk_schema_version_, const SchemaContext & schema_ctx_);

    void read(DB::Block & res, DB::ReadBuffer & istr, bool partial) const;

private:
    inline void readFullForRequestFull(DB::ReadBuffer & istr, uint32_t rows, DB::Block & res) const;
    inline void readFullForRequestPartial(DB::ReadBuffer & istr, uint32_t rows, DB::Block & res) const;
    inline void readPartialForRequestFull(DB::ReadBuffer & istr, uint16_t columns, uint32_t rows, DB::Block & res) const;
    inline void readPartialForRequestPartial(DB::ReadBuffer & istr, uint16_t columns, uint32_t rows, DB::Block & res) const;

    void initDeserializationContext();

private:
    const uint16_t on_disk_schema_version;
    const SchemaContext & schema_ctx;

    DB::Block on_disk_schema;
    /// Requested columns.
    DB::Block requested_header;

    /// Pre-build deserialization information
    DeserializationContext serde_ctx;
};

}
