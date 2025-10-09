#pragma once

#include <Cluster/SchemaRecord/SchemaContext.h>

namespace cluster
{
class SchemaContextProvider
{
public:
    virtual const SchemaContext & getSchemaContext(const std::string & stream, const DB::UUID & stream_id) const = 0;
    virtual ~SchemaContextProvider() = default;
};
}
