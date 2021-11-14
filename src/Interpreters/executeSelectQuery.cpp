/// #include <DataStreams/AsynchronousBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

namespace DB
{
void executeNonInsertQuery(const String & query, ContextMutablePtr query_context, const std::function<void(Block &&)> & callback, bool internal)
{
    BlockIO io{executeQuery(query, query_context, internal)};

    assert (!io.pipeline.pushing());

    if (io.pipeline.pulling())
    {
        DB::PullingAsyncPipelineExecutor executor(io.pipeline);
        DB::Block block;

        while (executor.pull(block, 100))
        {
            if (block && callback)
            {
                callback(std::move(block));
            }
        }
    }
    else if (io.pipeline.completed())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
        if (callback)
        {
            callback({});
        }
    }
    else
    {
        if (callback)
        {
            callback({});
        }
    }

    io.onFinish();
}
}
