/// #include <DataStreams/AsynchronousBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
void executeNonInsertQuery(
    const String & query, ContextMutablePtr query_context, const std::function<void(Block &&)> & callback, bool internal)
{
    BlockIO io{executeQuery(query, query_context, internal)};

    chassert(!io.pipeline.pushing());

    if (io.pipeline.pulling())
    {
        DB::PullingAsyncPipelineExecutor executor(io.pipeline);
        DB::Block block;

        while (executor.pull(block, 100))
        {
            if (block && callback)
                callback(std::move(block));
        }
    }
    else if (io.pipeline.completed())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
        if (callback)
            callback({});
    }
    else
    {
        if (callback)
            callback({});
    }

    io.onFinish();
}

void executeSelectPipeline(QueryPipeline pipeline, const std::function<void(Chunk &&)> & callback)
{
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling");

    PullingAsyncPipelineExecutor executor(pipeline);

    Chunk chunk;
    while (executor.pull(chunk, 100))
    {
        if (chunk)
            callback(std::move(chunk));
    }

    callback(Chunk{}); /// Empty chunk to signal the end
}

void executeSelectPipeline(Pipe pipe, const std::function<void(Chunk &&)> & callback)
{
    QueryPipeline pipeline{std::move(pipe)};
    executeSelectPipeline(std::move(pipeline), callback);
}

void executeSelectPipeline(QueryPlan query_plan, ContextPtr query_context, const std::function<void(Chunk &&)> & callback)
{
    auto builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(query_context), BuildQueryPipelineSettings::fromContext(query_context), query_context);

    executeSelectPipeline(QueryPipelineBuilder::getPipeline(std::move(*builder)), callback);
}
}
