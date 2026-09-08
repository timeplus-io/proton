#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

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

/// Dotted Iceberg field path (t.x, arr.element, m.key, m.value) -> field id.
using FieldIdsByPath = std::unordered_map<std::string, int64_t>;

/// Collects the field id of every field, nested ones included, of an Iceberg `struct` schema.
FieldIdsByPath getFieldIdsByPath(const Poco::JSON::Object & iceberg_schema);
}
