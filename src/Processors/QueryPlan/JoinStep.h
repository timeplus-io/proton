#pragma once

#include <Core/Joins.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/// proton: starts. Inner/Left/Cross/Comma anchor every output row to its left row's shuffle
/// keys; Right/Full NULL-fill left keys on unmatched-right rows, breaking same-key-same-stream.
/// Kind alone decides: strictness keeps rows left-anchored, and streaming/changelog joins (which
/// could break this) route to Streaming::JoinStep, which never propagates.
bool joinPreservesLeftShuffle(JoinKind kind);
/// proton: ends.

/// Join two data streams.
class JoinStep : public IQueryPlanStep
{
public:
    JoinStep(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        size_t max_block_size_,
        size_t max_streams_,
        bool keep_left_read_in_order_);

    String getName() const override { return "Join"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }
    void setJoin(JoinPtr join_) { join = std::move(join_); }
    bool allowPushDownToRight() const;

    void updateInputStream(const DataStream & new_input_stream_, size_t idx);

private:
    JoinPtr join;
    size_t max_block_size;
    size_t max_streams;
    bool keep_left_read_in_order;
};

/// Special step for the case when Join is already filled.
/// For StorageJoin and Dictionary.
class FilledJoinStep : public ITransformingStep
{
public:
    FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_);

    String getName() const override { return "FilledJoin"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

private:
    void updateOutputStream() override;

    JoinPtr join;
    size_t max_block_size;
};

}
