#pragma once

#include <cstdint>

namespace DB
{
/// Replicating checkpoints via different backends
enum class CheckpointReplicationType : uint8_t
{
    LocalFileSystem = 1
};
}
