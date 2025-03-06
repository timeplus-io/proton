#include <Formats/Avro/Schemas.h>

#if USE_AVRO

#include <Compiler.hh>

#include <fstream>

namespace DB
{

namespace Avro
{

avro::ValidSchema compileSchemaFromSchemaInfo(const FormatSchemaInfo & schema_info)
{
    std::ifstream ifs{schema_info.absoluteSchemaPath()};
    avro::ValidSchema schema;
    avro::compileJsonSchema(ifs, schema);
    return schema;
}

}

}

#endif
