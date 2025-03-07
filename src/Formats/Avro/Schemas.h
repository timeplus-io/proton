#pragma once

#include "config.h"
#if USE_AVRO

#include <Formats/FormatSchemaInfo.h>

#include <ValidSchema.hh>

namespace DB
{

namespace Avro
{

avro::ValidSchema compileSchemaFromSchemaInfo(const FormatSchemaInfo & schema_info);

}

}

#endif
