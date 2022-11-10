#pragma once

#include "AggregatingTransformWithSubstream.h"

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransform final : public AggregatingTransformWithSubstream
{
public:
    SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    SessionAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        SubstraemManyAggregatedDataPtr substream_many_data,
        size_t current_variant_,
        size_t max_threads,
        size_t temporary_data_merge_threads);

    ~SessionAggregatingTransform() override = default;

    String getName() const override { return "SessionAggregatingTransform"; }

private:
    void consume(Chunk chunk) override;
    void finalizeSession(const SessionInfo & info, Block & merged_block);
    void convertSingleLevel(ManyAggregatedDataVariantsPtr & data, const SessionInfo & info, Block & merged_block);
    SessionInfo & getOrCreateSessionInfo(const SessionID & session_id);
    SessionInfo & getSessionInfo(const SessionID & session_id);
    void resetSessionInfo(SessionInfo & info);

    template <typename TargetColumnType>
    std::pair<std::vector<IColumn::Filter>, SessionInfos> prepareSession(
        SessionInfo & info, ColumnPtr & time_column, ColumnPtr & session_start_column, ColumnPtr & session_end_column, size_t num_rows);
    void emitGlobalOversizeSessionsIfPossible(const Chunk & chunk, Block & merged_block);

    SubstreamHashMap<SessionInfoPtr> & session_map;
    Int64 max_event_ts = 0;
};
}
}
