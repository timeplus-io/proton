#pragma once

#include <Cluster/SchemaRecord/SchemaProvider.h>

namespace cluster
{
class ControlBlockSchemaProvider final : public SchemaProvider
{
public:
    explicit ControlBlockSchemaProvider(const DB::Block & schema_) : schema(schema_) { }

    const DB::Block & getSchema(const std::string & /*stream*/, const DB::UUID & /*stream_id*/, uint16_t /*schema_version*/) const override
    {
        return schema;
    }

private:
    const DB::Block & schema;
};

}
