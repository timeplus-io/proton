#pragma once

#include <memory>

namespace DB
{
struct ExtraCheckpointContext
{
    virtual ~ExtraCheckpointContext() = default;
};
using ExtraCheckpointContextPtr = std::shared_ptr<ExtraCheckpointContext>;
}
