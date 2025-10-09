#pragma once

#include <Interpreters/Streaming/Aggregator/IAggregator.h>

namespace DB::Streaming
{

struct AggregatingTransformParams
{
    IAggregatorPtr aggregator;
    IAggregatorParamsPtr params;

    bool emit_version;
    bool emit_changelog;
    bool emit_repeat; /// Only work for global aggregation for now
    bool emit_delta; /// Only work for global aggregation for now
    Streaming::EmitMode emit_mode;

    DataTypePtr version_type;
    bool keys_already_sharded;
    /// When aggregation keys are unique, it can help speed up the backfilling
    /// for hybrid aggregator
    bool aggregation_backfill_key_unique;

    AggregatingTransformParams(
        const Block & header_,
        const IAggregatorParamsPtr & params_,
        bool emit_version_,
        bool emit_changelog_,
        bool emit_repeat_,
        bool emit_delta_,
        Streaming::EmitMode emit_mode_,
        bool keys_already_sharded_,
        bool aggregation_backfill_key_unique_);

    AggregatorType aggregatorType() const noexcept { return aggregator->type(); }

    static Block getHeader(const Block & header, const IAggregatorParams & params, bool emit_version, bool emit_changelog);

    bool repeatEmit() const noexcept { return emit_repeat && !emit_changelog && emit_mode < EmitMode::OnUpdate; }
    bool deltaEmit() const noexcept { return emit_delta && !emit_changelog && emit_mode < EmitMode::OnUpdate; }

    bool emitOnExecute() const noexcept { return emit_mode == EmitMode::AfterKeyExpire || emit_mode == EmitMode::PerEvent; }

    Block getHeader() const;
};

using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

}
