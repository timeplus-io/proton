#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

namespace
{
void executeQueryWithProcessors(DB::QueryPipeline & pipeline, const std::function<void(DB::Block &&)> & callback)
{
    DB::PullingAsyncPipelineExecutor executor(pipeline);
    DB::Block block;

    while (executor.pull(block, 100))
    {
        if (block)
        {
            callback(std::move(block));
        }
    }
}

void executeQueryWithoutProcessor(DB::BlockInputStreamPtr & in, const std::function<void(DB::Block &&)> & callback)
{
    DB::AsynchronousBlockInputStream async_in(in);
    async_in.readPrefix();

    while (true)
    {
        if (async_in.poll(100))
        {
            DB::Block block{async_in.read()};
            if (!block)
            {
                break;
            }

            callback(std::move(block));
        }
    }
    async_in.readSuffix();
}

}

namespace DB
{
void executeSelectQuery(const String & query, ContextPtr query_context, const std::function<void(Block &&)> & callback, bool internal)
{
    BlockIO io{executeQuery(query, query_context, internal)};

    if (io.pipeline.initialized())
    {
        executeQueryWithProcessors(io.pipeline, callback);
    }
    else if (io.in)
    {
        executeQueryWithoutProcessor(io.in, callback);
    }
    io.onFinish();
}

}
