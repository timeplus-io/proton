#pragma once

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{
class Block;
class Chunk;
class QueryPlan;

void executeNonInsertQuery(
    const String & query, ContextMutablePtr query_context, const std::function<void(Block &&)> & callback, bool internal = true);

void executeSelectPipeline(QueryPipeline pipeline, const std::function<void(Chunk &&)> & callback);
void executeSelectPipeline(Pipe pipe, const std::function<void(Chunk &&)> & callback);
void executeSelectPipeline(QueryPlan query_plan, ContextPtr context, const std::function<void(Chunk &&)> & callback);
}
