#pragma once

#include <cstdint>

namespace DB
{
/// How to do checkpointing
struct CheckpointStrategy
{
    bool async : 1 {true};
    bool incremental : 1 {true};
    uint8_t unused : 6;
};
static_assert(sizeof(CheckpointStrategy) == 1);
}
