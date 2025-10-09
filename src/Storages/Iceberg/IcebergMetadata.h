#pragma once

#include "config.h"

#if USE_AVRO
#include <Storages/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Object.h>

namespace DB
{

class IcebergMetadata
{
public:
    static Int32 parseTableSchema(
        const Poco::JSON::Object::Ptr & metadata_object,
        IcebergSchemaProcessor & schema_processor,
        std::string & iceberg_schema_json,
        LoggerPtr metadata_logger);
};

}
#endif
