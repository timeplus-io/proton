#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <Processors/ISource.h>
#include <Common/serde.h>

#include <atomic>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
struct SequenceRange
{
    int64_t start;
    int64_t end;

public:
    int64_t count() const { return end - start + 1; }
};

class ISource : public DB::ISource
{
public:
    ISource(Block header, bool enable_auto_progress, LoggerPtr logger_, ProcessorID pid_);

    /// \return the current [low, high] sequence range of the source stream shard
    virtual std::pair<Int64, Int64> sequenceRange() const { return {-1, -1}; }

    /// \brief Get/Set the last progressed sequence number of the source
    Int64 lastProcessedSN() const noexcept { return last_processed_sn_end.load(std::memory_order_relaxed); }
    void setLastProcessedSN(Int64 sn) noexcept { setLastProcessedSNRange({sn, sn}); }

    /// \return last processed record timestamp which is attached to the last processed sn
    Int64 lastProcessedRecordTimestamp() const { return last_processed_record_timestamp.load(std::memory_order_relaxed); }
    void setLastProcessedRecordTimestamp(Int64 ts) { last_processed_record_timestamp = ts; }

    /// \brief Get/Set the last progressed sequence range of the source
    SequenceRange lastProcessedSNRange() const noexcept
    {
        return {last_processed_sn_start.load(std::memory_order_relaxed), last_processed_sn_end.load(std::memory_order_relaxed)};
    }
    void setLastProcessedSNRange(SequenceRange sn_range) noexcept
    {
        last_processed_sn_start.store(sn_range.start, std::memory_order_relaxed);
        last_processed_sn_end.store(sn_range.end, std::memory_order_relaxed);

        metrics.last_processed_sn_range = std::make_pair(sn_range.start, sn_range.end);
    }

    /// \brief Reset the start sequence number of the source, it must be called before the pipeline execution (thread-unsafe)
    void resetStartSN(Int64 sn);

    /// \brief Get/Set the last checkpoint sequence number of the source
    Int64 lastCheckpointSN() const noexcept { return last_ckpt_sn.load(std::memory_order_relaxed); }
    void setLastCheckpointSN(Int64 ckpt_sn) noexcept { last_ckpt_sn.store(ckpt_sn, std::memory_order_relaxed); }

    bool hasProcessedNewDataSinceLastCheckpoint() const noexcept { return lastProcessedSN() > lastCheckpointSN(); }

    bool hasState() const override { return true; }
    /// Source-side checkpoint entry: schedules a request; persistence happens later in tryGenerate.
    void triggerCheckpoint(CheckpointContextPtr ckpt_ctx_);
    void recover(CheckpointContextPtr ckpt_ctx_) override final;

    struct Metrics
    {
        String name;
        String description;
        Int64 processed_record_ts;
        SequenceRange processed_sn_range;
        Int64 ckpt_sn;
        std::pair<Int64, Int64> low_high_sn_range;

        String toString() const;
    };
    std::shared_ptr<Metrics> getMetrics() const;

    /// Fetch data for the specific SN range. The data must be serialized into bytes. How the data are encoded is up to the source.
    Strings fetchData(const SequenceRange &);

private:
    std::optional<Chunk> tryGenerate() override;

    /// \brief Checkpointing the source state (include lastProcessedSN())
    void doCheckpoint(CheckpointContextPtr) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for checkpoting of {}", getName());
    }

    /// \brief Recovering the source state (include lastCheckpointSN())
    void doRecover(CheckpointContextPtr) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for recovering of {}", getName());
    }

    /// \brief Reset current consume sequence number, it must be called before the pipeline execution (thread-unsafe)
    virtual void doResetStartSN(Int64 /*sn*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for doResetStartSN() of {}", getName());
    }

    virtual Strings doFetchData(const SequenceRange &) { return {}; }

protected:
    LoggerPtr logger;

private:
    /// For checkpoint
    CheckpointRequest ckpt_request;
    NO_SERDE std::optional<Int64> reset_start_sn;
    NO_SERDE std::atomic<Int64> last_ckpt_sn = -1;
    NO_SERDE std::atomic<Int64> last_processed_sn_start = -1;
    NO_SERDE std::atomic<Int64> last_processed_record_timestamp = 0;
    SERDE std::atomic<Int64> last_processed_sn_end = -1;

    static constexpr Int64 log_metrics_interval_ms = 30'000;
    Int64 last_log_ts = 0;
};

using StreamingSourcePtr = std::shared_ptr<ISource>;
using StreamingSourcePtrs = std::vector<StreamingSourcePtr>;
using StreamingSourceMetricsPtr = std::shared_ptr<ISource::Metrics>;
using StreamingSourceMetricsPtrs = std::vector<StreamingSourceMetricsPtr>;
}
}
