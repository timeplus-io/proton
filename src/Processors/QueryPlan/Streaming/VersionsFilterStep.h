#pragma once

#include <Core/HashTableType.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
class VersionsFilterStep final : public ITransformingStep
{
public:
    VersionsFilterStep(
        const DataStream & input_stream_,
        Block output_header_,
        std::vector<std::string> key_column_names_,
        const std::string & version_column_name_,
        HashTableType hash_table_type_,
        const std::string & spill_dir_,
        size_t max_hot_keys_,
        bool backfill_key_unique_);

    String getName() const override;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;

    std::vector<std::string> key_column_names;
    std::string version_column_name;
    HashTableType hash_table_type;
    std::string spill_dir;
    size_t max_hot_keys;
    bool backfill_key_unique;
};

}
}
