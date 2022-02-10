#pragma once

#include <Core/Block.h>


namespace DWAL
{
constexpr uint16_t NO_SCHEMA = std::numeric_limits<uint16_t>::max();

struct SchemaProvider
{
    virtual const DB::Block & getSchema(uint16_t schema_version) const = 0;
    virtual ~SchemaProvider() = default;
};

struct EmptySchemaProvider : public SchemaProvider
{
    const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

private:
    DB::Block header;
};
};
