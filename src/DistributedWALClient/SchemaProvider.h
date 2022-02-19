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


struct SchemaContext
{
    SchemaContext(): empty_schema_provider(new EmptySchemaProvider), schema_provider(empty_schema_provider.get()) { }
    explicit SchemaContext(const SchemaProvider & provider): schema_provider(&provider) { }

    std::shared_ptr<EmptySchemaProvider> empty_schema_provider;
    const SchemaProvider * schema_provider;
    uint16_t read_schema_version = 0;
    std::vector<uint16_t> column_positions;
};

};
