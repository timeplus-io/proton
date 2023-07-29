#pragma once

#include <Columns/IColumn.h>

namespace DB
{
enum class HashType;

namespace Streaming
{
/// Choose best hash method for key columns
/// @return hash type and key sizes pair
std::pair<HashType, std::vector<size_t>> chooseHashMethod(const ColumnRawPtrs & key_columns);
}
}
