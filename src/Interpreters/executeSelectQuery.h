#pragma once

#include <Core/Block.h>

namespace DB
{
void executeNonInsertQuery(
    const String & query, ContextMutablePtr query_context, const std::function<void(Block &&)> & callback, bool internal = true);
}
