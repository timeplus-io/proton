#pragma once

namespace DB::Streaming
{

enum AggregatorType : uint8_t
{
    Memory = 0,
    Hybrid = 1,
};

}
