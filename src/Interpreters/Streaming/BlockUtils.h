#pragma once

#include "Interpreters/Context_fwd.h"

#include <Core/Block.h>
#include <NativeLog/Record/OpCodes.h>

namespace DB
{
namespace Streaming
{
Block buildBlock(
    const std::vector<std::pair<String, String>> & string_cols,
    const std::vector<std::pair<String, Int32>> & int32_cols,
    const std::vector<std::pair<String, UInt64>> & uint64_cols);

Block buildBlock(
    const std::vector<std::pair<String, std::vector<String>>> & string_cols,
    const std::vector<std::pair<String, std::vector<Int64>>> & int64_cols);
}
}
