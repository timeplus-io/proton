#pragma once

#include <Checkpoint/CheckpointConfig.h>
#include <Checkpoint/CheckpointStorage.h>

namespace DB
{
struct CheckpointStorageFactory final
{
    static std::unique_ptr<CheckpointStorage> create(CheckpointReplicationType type, const CheckpointConfig & config);
};
}
