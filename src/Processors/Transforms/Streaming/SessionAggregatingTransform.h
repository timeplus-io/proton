#pragma once

#include "AggregatingTransform.h"
#include "SessionHelper.h"

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransform final : public AggregatingTransform
{
public:
    SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    ~SessionAggregatingTransform() override = default;

    String getName() const override { return "SessionAggregatingTransform"; }

private:
    void consume(Chunk chunk) override;

    void finalizeSession(const SessionInfo & info, Block & merged_block);

private:
    SessionInfo session_info;
};
}
}
