#pragma once

#include <cstdint>

namespace DB
{
enum class CheckpointType : uint8_t
{
    Auto = 0,
    File = 1,
    Rocks = 2,
};
}
