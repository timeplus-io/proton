#include <Processors/Streaming/ISource.h>

#include <Checkpoint/CheckpointContext.h>
#include <base/ClockUtils.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>

namespace DB::Streaming
{
ISource::ISource(Block header, bool enable_auto_progress, LoggerPtr logger_, ProcessorID pid_)
    : DB::ISource(std::move(header), enable_auto_progress, pid_), logger(logger_), last_log_ts(MonotonicMilliseconds::now())
{
    is_streaming = true;
}

/// Called from CheckpointCoordinator thread; ckpt_request handles cross-thread handoff to tryGenerate.
void ISource::triggerCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    chassert(hasState());
    ckpt_request.setCheckpointRequestCtx(std::move(ckpt_ctx_));
}

void ISource::recover(CheckpointContextPtr ckpt_ctx_)
{
    doRecover(std::move(ckpt_ctx_));
    auto recovered_sn = lastCheckpointSN();

    /// Reset consume offset started from the next of last checkpoint sn (if not manually reset before recovery)
    if (!reset_start_sn.has_value() && recovered_sn >= 0)
    {
        doResetStartSN(recovered_sn + 1);
        setLastProcessedSN(recovered_sn);
    }
}

void ISource::resetStartSN(Int64 sn)
{
    reset_start_sn = sn;
    doResetStartSN(sn);
    if (sn > 0)
        setLastProcessedSN(sn - 1);
}

std::optional<Chunk> ISource::tryGenerate()
{
    SCOPE_EXIT({
        if (auto now = MonotonicMilliseconds::now(); now - last_log_ts > log_metrics_interval_ms)
        {
            LOG_INFO(logger, "{}", getMetrics()->toString());
            last_log_ts = now;
        }
    });

    if (auto current_ckpt_ctx = ckpt_request.poll(); current_ckpt_ctx)
    {
        doCheckpoint(current_ckpt_ctx);
        /// Update in-memory checkpoint sn after finishing the current ckpt epoch
        current_ckpt_ctx->registerFinishCallback(
            [this, ckpt_sn = lastProcessedSN()](CheckpointContextPtr) { setLastCheckpointSN(ckpt_sn); });

        Chunk barrier(getPort().getHeader().getColumns(), 0);
        barrier.setCheckpointContext(std::move(current_ckpt_ctx));
        return barrier;
    }

    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}

std::shared_ptr<ISource::Metrics> ISource::getMetrics() const
{
    return std::make_shared<Metrics>(Metrics{
        .name = getName(),
        .description = getDescription(),
        .processed_record_ts = lastProcessedRecordTimestamp(),
        .processed_sn_range = lastProcessedSNRange(),
        .ckpt_sn = lastCheckpointSN(),
        .low_high_sn_range = sequenceRange(),
    });
}

String ISource::Metrics::toString() const
{
    return fmt::format(
        "{}[{}]: last_processed_ts={}, last_processed_sn_range=[{}, {}], last_checkpointed_sn={}, low_high_sn_range=[{}, {}]",
        name,
        description,
        processed_record_ts,
        processed_sn_range.start,
        processed_sn_range.end,
        ckpt_sn,
        low_high_sn_range.first,
        low_high_sn_range.second);
}

Strings ISource::fetchData(const SequenceRange & sn_range)
{
    /// fetchData is used to fetch bad data for DLQ by materialized views, and materialized views will cancel the pipeline before calling this function.
    /// Making sure that the source is already cancelled when this function is called, it could make the implementation of doFetchData a lot easier.
    /// That means, if we are going to change this contract, the doFetchData functions on sources might need to update as well.
    if (!isCancelled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function fetchData is supposed to be called on a cancelled source");

    return doFetchData(sn_range);
}

}
