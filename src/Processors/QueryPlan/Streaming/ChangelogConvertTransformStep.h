#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
class ChangelogConvertTransformStep final : public ITransformingStep
{
public:
    ChangelogConvertTransformStep(
        const DataStream & input_stream_,
        Block output_header_,
        std::vector<std::string> key_column_names_,
        const std::string & version_column_name_,
        size_t max_thread_);

    String getName() const override { return "ChangelogConvertTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::vector<std::string> key_column_names;
    std::string version_column_name;
    [[maybe_unused]] size_t max_thread;
};

}
}
