#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <vector>

namespace DB
{

/// Best-effort runtime fallback for retrieving INPUT target streams.
/// Returns unqualified stream names when the target is in the same database.
std::vector<String> tryGetInputTargetStreamsFromRuntime(const String & database, const String & input, ContextPtr context);

}
