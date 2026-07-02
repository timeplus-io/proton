#include "WatermarkStepWithSubstream.h"

#include <Processors/Transforms/Streaming/WatermarkTransformWithSubstream.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Streaming
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
            .preserves_shuffling = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

WatermarkStepWithSubstream::WatermarkStepWithSubstream(
    const DataStream & input_stream_,
    EmitParamsPtr params_,
    bool skip_stamping_for_backfill_data_,
    HashTableType hash_table_type_,
    const String & spill_dir_,
    size_t max_hot_key_count_,
    const String & kv_options_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , params(std::move(params_))
    , skip_stamping_for_backfill_data(skip_stamping_for_backfill_data_)
    , hash_table_type(hash_table_type_)
    , spill_dir(spill_dir_)
    , kv_options(kv_options_)
    , max_hot_key_count(max_hot_key_count_)
{
}

void WatermarkStepWithSubstream::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    size_t transform_id = 0;
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<WatermarkTransformWithSubstream>(
            header,
            params,
            skip_stamping_for_backfill_data,
            hash_table_type,
            fmt::format("{}-{}", spill_dir, transform_id++),
            max_hot_key_count,
            kv_options);
    });
}

void WatermarkStepWithSubstream::updateOutputStream()
{
    output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
}
}
}
