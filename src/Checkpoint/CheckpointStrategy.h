#pragma once

#include <cstdint>

namespace DB
{
/// How to do checkpointing
struct CheckpointStrategy
{
    bool async : 1 {true};
    bool incremental : 1 {true};
    bool offsets_only : 1 {false};
    uint8_t unused : 5;
};
static_assert(sizeof(CheckpointStrategy) == 1);
}
