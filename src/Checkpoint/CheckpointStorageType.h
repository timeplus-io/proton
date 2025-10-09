#pragma once

#include <cstdint>

namespace DB
{
/// Where to store checkpoints
enum class CheckpointStorageType : uint8_t
{
    LocalFileSystem = 1
};
}
