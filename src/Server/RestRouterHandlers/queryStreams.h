#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{
class Block;

void queryStreams(ContextMutablePtr query_context, const std::function<void(Block &&)> & callback);
void queryOneStream(
    ContextMutablePtr query_context, const String & database_name, const String & name, const std::function<void(Block &&)> & callback);
void queryStreamsByDatabase(ContextMutablePtr query_context, const String & database_name, const std::function<void(Block &&)> & callback);
}
