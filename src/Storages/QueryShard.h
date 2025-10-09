#pragma once

#include <base/types.h>

namespace DB
{
enum class QueryMode : uint8_t
{
    Streaming, /// streaming query
    StreamingConcat, /// streaming query with backfilled historical data
    Historical /// historical query
};

struct ShardsWithQueryMode
{
    QueryMode mode;
    std::vector<UInt64> shards;
};

}
