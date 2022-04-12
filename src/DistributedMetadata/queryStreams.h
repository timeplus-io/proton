#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
void queryStreams(ContextMutablePtr query_context, const std::function<void(Block &&)> & callback);
}
