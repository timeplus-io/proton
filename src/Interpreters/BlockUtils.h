#pragma once

#include "Context_fwd.h"

#include <Core/Block.h>
#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>

namespace DB
{
Block buildBlock(
    const std::vector<std::pair<String, String>> & string_cols,
    const std::vector<std::pair<String, Int32>> & int32_cols,
    const std::vector<std::pair<String, UInt64>> & uint64_cols);

Block buildBlock(
    const std::vector<std::pair<String, std::vector<String>>> & string_cols,
    const std::vector<std::pair<String, std::vector<Int64>>> & int64_cols);

void appendBlock(Block && block, ContextPtr context, IDistributedWriteAheadLog::OpCode opCode, const Poco::Logger * log);

}
