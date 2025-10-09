#pragma once

#include <string>

namespace DB
{

/// Convert Timeplus (complex / compound) type names to ClickHouse data type names (recursively)
std::string convertToClickHouseType(const std::string & type_name);
}
