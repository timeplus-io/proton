#pragma once

#include <Core/HashTableType.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB::Streaming
{
class DedupTransformStep final : public ITransformingStep
{
public:
    DedupTransformStep(
        const DataStream & input_stream_,
        Block output_header,
        TableFunctionDescriptionPtr dedup_func_desc_,
        HashTableType hash_table_type_,
        const String & spill_dir_);

    ~DedupTransformStep() override = default;

    String getName() const override { return "DedupTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputStream() override;

    TableFunctionDescriptionPtr dedup_func_desc;
    HashTableType hash_table_type;
    String spill_dir;
    size_t id = 0;
};

}
