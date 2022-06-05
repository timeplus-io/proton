#pragma once

#include <Core/Block.h>
#include <Storages/Streaming/SourceColumnsDescription.h>

namespace nlog
{
constexpr uint16_t NO_SCHEMA = std::numeric_limits<uint16_t>::max();
constexpr uint16_t ALL_SCHEMA = std::numeric_limits<uint16_t>::max() - 1;

constexpr uint16_t NO_POSITION = std::numeric_limits<uint16_t>::max();

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
    SchemaContext() : empty_schema_provider(new EmptySchemaProvider), schema_provider(empty_schema_provider.get()) { }

    explicit SchemaContext(const SchemaProvider & provider) : schema_provider(&provider) { }

    SchemaContext(const SchemaProvider & provider, uint16_t schema_version_, DB::SourceColumnsDescription::PhysicalColumnPositions column_positions_)
        : schema_provider(&provider), read_schema_version(schema_version_), column_positions(std::move(column_positions_))
    {
    }

    std::shared_ptr<EmptySchemaProvider> empty_schema_provider;
    const SchemaProvider * schema_provider;
    uint16_t read_schema_version = 0;
    DB::SourceColumnsDescription::PhysicalColumnPositions column_positions;
};

};
