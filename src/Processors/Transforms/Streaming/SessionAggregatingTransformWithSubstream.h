#pragma once

#include "AggregatingTransformWithSubstream.h"
#include "SessionHelper.h"

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransformWithSubstream : public AggregatingTransformWithSubstream
{
public:
    SessionAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);
    ~SessionAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "SessionAggregatingTransformWithSubstream"; }

protected:
    SubstreamContextPtr getOrCreateSubstreamContext(const SubstreamID & id) override;

private:
    void consume(Chunk chunk, const SubstreamContextPtr & substream_ctx) override;

    void finalizeSession(const SubstreamContextPtr & substream_ctx, const SessionInfo & info, Block & merged_block);

    void emitGlobalOversizeSessionsIfPossible(const Chunk & chunk, Block & merged_block);

private:
    Int64 max_event_ts = 0;
};
}
}
