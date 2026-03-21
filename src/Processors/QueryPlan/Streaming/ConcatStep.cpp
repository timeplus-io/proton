#include <Processors/QueryPlan/Streaming/ConcatStep.h>
#include <Processors/Streaming/ISource.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/logger_useful.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
static Block checkHeaders(const DataStreams & input_streams)
{
    if (input_streams.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot concat {} query plan steps, expected 2", input_streams.size());

    Block res = input_streams.front().header;
    assertBlocksHaveEqualStructure(res, input_streams.back().header, "ConcatStep");

    return res;
}

ConcatStep::ConcatStep(DataStreams input_streams_, bool disable_concat_callback_, UInt64 backfill_max_threads_)
    : header(checkHeaders(input_streams_)), disable_concat_callback(disable_concat_callback_), backfill_max_threads(backfill_max_threads_)
{
    input_streams = std::move(input_streams_);
    output_stream = DataStream{.header = header, .is_streaming = true};
}

QueryPipelineBuilderPtr ConcatStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    chassert(pipelines.size() == 2);
    /// For backfill concat, a common example:
    ///                                   _________________
    ///  historical inputs  =========>   |                 |
    ///                                  |                 |
    ///                     (Delayed)    | StreamingConcat | ======>  streaming outputs
    ///                                  |                 |
    ///  streaming  inputs  =========>   |_________________|

    auto historical_pipeline = std::move(pipelines.front());
    chassert(!historical_pipeline->isStreaming());

    auto streaming_pipeline = std::move(pipelines.back());
    chassert(streaming_pipeline->isStreaming());

    std::function<void(Int64)> concat_callback;
    if (!disable_concat_callback)
    {
        concat_callback = [sources = streaming_pipeline->getStreamingSources()](Int64 historical_max_sn) {
            if (historical_max_sn >= cluster::Constants::LogStartSN)
            {
                for (const auto & source : sources)
                    source->resetStartSN(historical_max_sn + 1);
            }

            LOG_INFO(
                getLogger("StreamingConcat"),
                "Concat to {}, historical_max_sn={}",
                fmt::join(sources | std::views::transform([](const auto & source) { return source->getMetrics()->toString(); }), ";"),
                historical_max_sn);
        };
    }

    /// Concat historical pipeline and streaming pipeline via Streaming::ConcatProcessor, which will pulls all data from historical inputs,
    /// then get max_sn from historical inputs to reset streaming source, and then read all data from streaming inputs.
    return QueryPipelineBuilder::concatPipelines(
        std::move(historical_pipeline), std::move(streaming_pipeline), std::move(concat_callback), &processors, backfill_max_threads);
}

void ConcatStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}
}
}
