#pragma once

#include "StreamingAggregatingTransform.h"

namespace DB
{
class SessionAggregatingTransform final : public StreamingAggregatingTransform
{
public:
    SessionAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    SessionAggregatingTransform(
        Block header,
        StreamingAggregatingTransformParamsPtr params_,
        ManyStreamingAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~SessionAggregatingTransform() override = default;

    String getName() const override { return "SessionAggregatingTransform"; }

private:
    void consume(Chunk chunk) override;
    void finalizeSession(std::vector<size_t> & sessions, Block & merged_block);
    void
    mergeTwoLevel(ManyStreamingAggregatedDataVariantsPtr & data, const std::vector<size_t> & sessions, Block & merged_block);
    void removeBuckets(std::vector<size_t> & sessions);
};

}
