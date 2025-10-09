#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{
class CheckpointCoordinator;

CheckpointCoordinator & bootstrapStateCheckpointCoordinator(const ContextPtr & global_context);
}
