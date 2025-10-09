#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
class ChangelogStep final : public ITransformingStep
{
public:
    ChangelogStep(
        const DataStream & input_stream_,
        Block output_header_,
        std::vector<std::string> key_column_names_,
        const std::string & version_column_name_);

    String getName() const override { return "ChangelogStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;

    std::vector<std::string> key_column_names;
    std::string version_column_name;
};

}
}
