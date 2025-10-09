#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
struct StorageID;
using Names = std::vector<std::string>;

ASTPtr buildSelectFromTable(const StorageID & table_id, const Names & column_names);
}
