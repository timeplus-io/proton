#pragma once

#include <Storages/Stream/StreamingBlockReaderBase.h>
#include <Cluster/Common/FetchHint.h>

namespace Poco
{
class Logger;
}

namespace cluster::client
{
class Client;
}

namespace DB
{

struct HistoricalQueryContext;
class StreamingBlockReaderNativeLog final : public StreamingBlockReaderBase
{
public:
    /// \param sn fetch start sequence number
    /// \param max_wait_ms_ max wait time for new data per fetch call if there is none
    /// \param max_bytes_ max bytes per fetch
    /// \param queued_max_bytes_ queued bytes for prefetch
    StreamingBlockReaderNativeLog(
        Int64 sn,
        Int64 max_wait_ms_,
        UInt64 max_bytes_,
        UInt64 queued_max_bytes_,
        const cluster::StreamShard & stream_shard_,
        const IStorage * storage,
        Block sample_block,
        UInt16 query_schema_version,
        cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions_,
        std::function<bool()> is_stopped_,
        bool is_local_,
        LoggerPtr logger_);

    ~StreamingBlockReaderNativeLog() noexcept override;

    cluster::SchemaRecordPtrs read() override;

    std::pair<Int64, Int64> sequenceRange() const override { return {log_start_sn, log_committed_sn}; }

    /// Call this function only before read()
    void resetSequenceNumber(Int64 sn) override;

    void setEmitControlBlock(bool emit_control_block_) noexcept { emit_control_block = emit_control_block_; }

    void setAllowFallbackToHistoricalStore(bool allow_fallback_to_historical_store_) noexcept
    {
        allow_fallback_to_historical_store = allow_fallback_to_historical_store_;
    }

    void setAvoidFillDefaultsForMissingColumns(bool avoid_fill_defaults_for_missing_columns_) noexcept
    {
        schema_ctx->setAvoidFillDefaultsForMissingColumns(avoid_fill_defaults_for_missing_columns_);
    }

private:
    void deserializeRecords(const cluster::EntryPtrs & entries, cluster::SchemaRecordPtrs & results);
    void startFetch(Int64 sn);

    void buildHistoricalQueryContext(Int64 fetch_range, Int64 log_start_sn, Int64 log_committed_sn);
    cluster::SchemaRecordPtrs readFromHistoricalStore();

private:
    // Direct NativeLog fetch context
    struct FetchContext
    {
        cluster::StreamShard stream_shard;
        int64_t sn;
        int64_t max_wait_ms;
        int64_t max_size;
        std::optional<cluster::FetchHint> fetch_hint;
    };
    FetchContext fetch_ctx;

    Int64 max_wait_ms;
    UInt64 max_bytes;
    UInt64 queued_max_bytes;
    bool is_local;

    /// control block can co-exist with data block in the same nativelog shard, and have different
    /// encoding / schema, for user facing query processing pipeline, control block shall be filtered
    /// out since it is completely used for internal purpose like alter partition, mutate data etc
    bool emit_control_block = false;

    Int64 log_start_sn = 0;
    Int64 log_committed_sn = 0;
    Int64 fetched_sn = 0;

    static constexpr Int64 default_fetch_range = 10'000;
    bool allow_fallback_to_historical_store = true;
    std::shared_ptr<HistoricalQueryContext> historical_ctx;
};
}
