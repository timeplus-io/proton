#pragma once

#include <memory>

namespace DB
{
struct CheckpointRequestContext;
using CheckpointRequestContextPtr = std::shared_ptr<CheckpointRequestContext>;

struct CheckpointContext;
using CheckpointContextPtr = std::shared_ptr<const CheckpointContext>;
}
