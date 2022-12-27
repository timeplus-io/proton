#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
void queryStreams(ContextMutablePtr query_context, const std::function<void(Block &&)> & callback);
void queryOneStream(ContextMutablePtr query_context, const String &database_name, const String &name, const std::function<void(Block &&)> & callback);
void queryStreamsByDatabasse(ContextMutablePtr query_context, const String &database_name, const std::function<void(Block &&)> & callback);
}
