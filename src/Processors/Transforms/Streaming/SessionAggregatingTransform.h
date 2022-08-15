#pragma once

#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransform final : public AggregatingTransform
{
public:
    SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    SessionAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~SessionAggregatingTransform() override = default;

    String getName() const override { return "SessionAggregatingTransform"; }

private:
    void consume(Chunk chunk) override;
    void finalizeSession(std::vector<size_t> & sessions, Block & merged_block);
    void mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, const std::vector<size_t> & sessions, Block & merged_block);
    void removeBuckets(std::vector<size_t> & sessions);
};

}
}
