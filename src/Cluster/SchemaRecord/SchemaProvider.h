#pragma once

#include <Cluster/Protocol/InternalProtocol.h>
#include <Core/Block.h>
#include <Common/Exception.h>

namespace DB
{
class ColumnsDescription;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}
}

namespace cluster
{
/// SchemaProvider is implemented to support multiple version.
struct SchemaProvider
{
    virtual const DB::Block & getSchema(const std::string & stream, const DB::UUID & stream_id, uint16_t schema_version) const = 0;

    virtual const DB::ColumnsDescription & getColumnsDescription(uint16_t schema_version) const
    {
        (void)schema_version;
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "SchemaProvider::getColumnsDescription is not implemented");
    }

    /// Convert source block from source schema version to target schema version
    virtual void convertBlock(DB::Block & source, uint16_t source_schema_version, uint16_t target_schema_version) const
    {
        (void)source;
        (void)source_schema_version;
        (void)target_schema_version;
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "SchemaProvider::convertBlock is not implemented");
    }

    virtual ~SchemaProvider() = default;
};

struct EmptySchemaProvider final : public SchemaProvider
{
    const DB::Block & getSchema(const std::string & /*stream*/, const DB::UUID & /*stream_id*/, uint16_t /*schema_version*/) const override
    {
        return header;
    }

    static EmptySchemaProvider & instance()
    {
        static EmptySchemaProvider provider;
        return provider;
    }

private:
    DB::Block header;
};

};
