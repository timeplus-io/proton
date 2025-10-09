#pragma once

namespace DB
{

enum class AggregatingConvertType : uint8_t
{
    Normal = 0,
    Updates = 1,
    Retract = 2,
    UpdatesAfterRetract = 3,
};

}
