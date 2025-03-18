#pragma once

namespace DB
{
class NamesAndTypesList;
}

namespace Poco::JSON
{
class Object;
}

namespace Apache::Iceberg
{
Poco::JSON::Object generateIcebergSchema(const DB::NamesAndTypesList & proton_schema);
}
