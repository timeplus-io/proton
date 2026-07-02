#include <Processors/QueryPlan/Streaming/ChangelogConvertStep.h>
#include <Processors/Transforms/Streaming/ChangelogConvertTransform.h>
#include <Processors/Transforms/Streaming/HybridChangelogConvertTransform.h>
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
            .preserves_sorting = false,
            .preserves_shuffling = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

ChangelogConvertStep::ChangelogConvertStep(
    const DataStream & input_stream_,
    Block output_header_,
    std::vector<std::string> key_column_names_,
    const std::string & version_column_name_,
    bool late_insert_overrides_,
    HashTableType hash_table_type_,
    const std::string & spill_dir_,
    size_t max_hot_keys_,
    const std::string & kv_options_,
    bool backfill_key_unique_)
    : ITransformingStep(input_stream_, ChangelogConvertTransform::transformOutputHeader(output_header_), getTraits())
    , key_column_names(std::move(key_column_names_))
    , version_column_name(version_column_name_)
    , late_insert_overrides(late_insert_overrides_)
    , hash_table_type(hash_table_type_)
    , spill_dir(spill_dir_)
    , kv_options(kv_options_)
    , max_hot_keys(max_hot_keys_)
    , backfill_key_unique(backfill_key_unique_)
{
}

void ChangelogConvertStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    size_t transform_id = 0;
    pipeline.addSimpleTransform([&](const Block & input_header) -> std::shared_ptr<IProcessor> {
        switch (hash_table_type)
        {
            case HashTableType::Memory:
                return std::make_shared<ChangelogConvertTransform>(
                    input_header,
                    getOutputStream().header,
                    key_column_names,
                    version_column_name,
                    late_insert_overrides,
                    backfill_key_unique);
            case HashTableType::Hybrid:
                return std::make_shared<HybridChangelogConvertTransform>(
                    input_header,
                    getOutputStream().header,
                    key_column_names,
                    version_column_name,
                    late_insert_overrides,
                    fmt::format("{}-{}", spill_dir, transform_id++),
                    max_hot_keys,
                    kv_options,
                    backfill_key_unique);
        }
    });
}

void ChangelogConvertStep::updateOutputStream()
{
    auto & output_header = output_stream->header;
    output_stream = createOutputStream(input_streams.front(), std::move(output_header), getDataStreamTraits());
}


String ChangelogConvertStep::getName() const
{
    switch (hash_table_type)
    {
        case HashTableType::Memory:
            return "ChangelogConvertStep";
        case HashTableType::Hybrid:
            return "HybridChangelogConvertStep";
    }
}
}
}
