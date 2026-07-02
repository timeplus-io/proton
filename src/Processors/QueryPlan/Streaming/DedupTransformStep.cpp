#include <Processors/QueryPlan/Streaming/DedupTransformStep.h>

#include <Processors/Transforms/Streaming/DedupTransform.h>
#include <Processors/Transforms/Streaming/HybridDedupTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB::Streaming
{
namespace
{
DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
            .preserves_shuffling = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

DedupTransformStep::DedupTransformStep(
    const DataStream & input_stream_,
    Block output_header,
    TableFunctionDescriptionPtr dedup_func_desc_,
    HashTableType hash_table_type_,
    const String & spill_dir_,
    const String & kv_options_)
    : ITransformingStep(input_stream_, std::move(output_header), getTraits())
    , dedup_func_desc(std::move(dedup_func_desc_))
    , hash_table_type(hash_table_type_)
    , spill_dir(spill_dir_)
    , kv_options(kv_options_)
{
}

void DedupTransformStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    /// Use one dedup transform for all input streams
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        if (hash_table_type == HashTableType::Memory)
            return std::make_shared<DedupTransform>(header, getOutputStream().header, dedup_func_desc);
        else
            return std::make_shared<HybridDedupTransform>(
                header, getOutputStream().header, dedup_func_desc, fmt::format("{}-{}", spill_dir, id++), kv_options);
    });
}

void DedupTransformStep::updateOutputStream()
{
    auto & output_header = output_stream->header;
    output_stream = createOutputStream(input_streams.front(), std::move(output_header), getDataStreamTraits());
}

}
