#include <Storages/Iceberg/IcebergMetadata.h>

#if USE_AVRO
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    if (!metadata_object->has("schema"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema' field is missing in metadata");
    Poco::JSON::Object::Ptr schema = metadata_object->getObject("schema");
    if (!schema->has("schema-id"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema-id' field is missing in schema");
    auto current_schema_id = schema->getValue<int>("schema-id");
    return {schema, current_schema_id};
}

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object)
{
    Poco::JSON::Object::Ptr schema;
    if (!metadata_object->has("current-schema-id"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'current-schema-id' field is missing in metadata");
    auto current_schema_id = metadata_object->getValue<int>("current-schema-id");
    if (!metadata_object->has("schemas"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schemas' field is missing in metadata");
    auto schemas = metadata_object->get("schemas").extract<Poco::JSON::Array::Ptr>();
    if (schemas->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: schemas field is empty");
    for (uint32_t i = 0; i != schemas->size(); ++i)
    {
        auto current_schema = schemas->getObject(i);
        if (!current_schema->has("schema-id"))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse Iceberg table schema: 'schema-id' field is missing in schema");
        }
        if (current_schema->getValue<int>("schema-id") == current_schema_id)
        {
            schema = current_schema;
            break;
        }
    }

    if (!schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(There is no schema with "schema-id" that matches "current-schema-id" in metadata)");
    if (schema->getValue<int>("schema-id") != current_schema_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(Field "schema-id" of the schema doesn't match "current-schema-id" in metadata)");
    return {schema, current_schema_id};
}

Int32 IcebergMetadata::parseTableSchema(
    const Poco::JSON::Object::Ptr & metadata_object,
    IcebergSchemaProcessor & schema_processor,
    std::string & iceberg_schema_json,
    LoggerPtr metadata_logger)
{
    Int32 format_version = metadata_object->getValue<Int32>("format-version");
    if (format_version == 2)
    {
        auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
        schema_processor.addIcebergTableSchema(schema);
        {
            std::stringstream ss;
            schema->stringify(ss);
            iceberg_schema_json = ss.str();
        }

        return current_schema_id;
    }
    else
    {
        try
        {
            auto [schema, current_schema_id] = parseTableSchemaV1Method(metadata_object);
            schema_processor.addIcebergTableSchema(schema);
            {
                std::stringstream ss;
                schema->stringify(ss);
                iceberg_schema_json = ss.str();
            }
            return current_schema_id;
        }
        catch (const Exception & first_error)
        {
            if (first_error.code() != ErrorCodes::BAD_ARGUMENTS)
                throw;
            try
            {
                auto [schema, current_schema_id] = parseTableSchemaV2Method(metadata_object);
                schema_processor.addIcebergTableSchema(schema);
                LOG_WARNING(
                    metadata_logger,
                    "Iceberg table schema was parsed using v2 specification, but it was impossible to parse it using v1 "
                    "specification. Be "
                    "aware that you Iceberg writing engine violates Iceberg specification. Error during parsing {}",
                    first_error.displayText());
                return current_schema_id;
            }
            catch (const Exception & second_error)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot parse Iceberg table schema both with v1 and v2 methods. V1 parsing error: {}. V2 parsing error: {}",
                    first_error.displayText(),
                    second_error.displayText());
            }
        }
    }
}

}

#endif
