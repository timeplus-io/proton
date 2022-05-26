#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

namespace DB
{
std::pair<Int64, Int64> columnMinMaxTimestamp(const ColumnPtr & column, const DataTypePtr & type);
}
