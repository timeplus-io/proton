#pragma once

#include <Core/HashTableType.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/Streaming/EmitParams.h>

namespace DB
{
namespace Streaming
{
/// Implement watermark assignment for streaming processing
class WatermarkStepWithSubstream final : public ITransformingStep
{
public:
    WatermarkStepWithSubstream(
        const DataStream & input_stream_,
        EmitParamsPtr params_,
        bool skip_stamping_for_backfill_data_,
        HashTableType hash_table_type_,
        const String & spill_dir_,
        size_t max_hot_key_count_);

    ~WatermarkStepWithSubstream() override = default;

    String getName() const override { return "WatermarkStepWithSubstream"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputStream() override;

    EmitParamsPtr params;
    bool skip_stamping_for_backfill_data;
    HashTableType hash_table_type;
    String spill_dir;
    size_t max_hot_key_count;
};
}
}
