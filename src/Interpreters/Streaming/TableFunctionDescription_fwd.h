#pragma once

#include <memory>
#include <vector>


namespace DB
{
namespace Streaming
{
struct TableFunctionDescription;
using TableFunctionDescriptionMutablePtr = std::shared_ptr<TableFunctionDescription>;
using TableFunctionDescriptionPtr = std::shared_ptr<const TableFunctionDescription>;
using FunctionDescriptionPtrs = std::vector<TableFunctionDescriptionPtr>;
}
}
