#pragma once

#include <memory>

namespace DB
{
struct CheckpointContext;
using CheckpointContextPtr = std::shared_ptr<const CheckpointContext>;
}
