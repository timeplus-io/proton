#include <Processors/Transforms/Streaming/AggregatingTransform.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_)
    : AggregatingTransform(std::move(header), std::move(params_), std::make_shared<ManyAggregatedData>(1), 0, 1, 1, log_name, pid_)
{
}

AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & log_name,
    ProcessorID pid_)
    : IProcessor({std::move(header)}, {params_->getHeader()}, pid_)
    , params(std::move(params_))
    , log(&Poco::Logger::get(log_name))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant_])
    , watermark_bound(many_data->watermarks[current_variant_])
    , ckpt_epoch(many_data->ckpt_epochs[current_variant_])
    , current_variant(current_variant_)
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
    (void)temporary_data_merge_threads;

    /// Register itself in the many aggregated data
    many_data->aggregating_transforms[current_variant] = this;
}

AggregatingTransform::~AggregatingTransform() = default;

IProcessor::Status AggregatingTransform::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished() || is_consume_finished)
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_input)
        return preparePushToOutput();

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        is_consume_finished = true;
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    current_chunk = input.pull(/*set_not_needed = */ true);
    read_current_chunk = true;

    return Status::Ready;
}

void AggregatingTransform::work()
{
    Int64 start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += current_chunk.bytes();

    if (likely(!is_consume_finished))
    {
        auto num_rows = current_chunk.getNumRows();
        if (num_rows > 0)
        {
            many_data->addRowCount(num_rows, current_variant);
            src_rows += num_rows;
            src_bytes += current_chunk.bytes();
        }

        consume(std::move(current_chunk));

        read_current_chunk = false;
    }

    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
}

void AggregatingTransform::consume(Chunk chunk)
{
    bool should_abort = false, need_finalization = false;

    auto num_rows = chunk.getNumRows();
    if (num_rows > 0)
    {
        /// There indeed has cases where num_rows of a chunk is greater than 0, but
        /// the columns are empty : select count() from stream where a != 0.
        /// So `executeOrMergeColumns` accepts num_rows as parameter
        if (std::tie(should_abort, need_finalization) = executeOrMergeColumns(chunk.detachColumns(), num_rows); should_abort)
            is_consume_finished = true;
    }

    /// Since checkpoint barrier is always standalone, it can't coexist with watermark,
    /// we handle watermark and checkpoint barrier separately
    if (chunk.hasWatermark() || need_finalization)
    {
        /// Watermark and need_finalization shall not be true at the same time
        /// since when UDA has user defined emit strategy, watermark is disabled
        assert(!(chunk.hasWatermark() && need_finalization));
        finalize(chunk.getChunkContext());
    }
    else if (chunk.requestCheckpoint())
        checkpointAlignment(chunk);
}

std::pair<bool, bool> AggregatingTransform::executeOrMergeColumns(Columns columns, size_t num_rows)
{
    if (params->only_merge)
    {
        auto block = getInputs().front().getHeader().cloneWithColumns(columns);
        materializeBlockInplace(block);
        /// FIXME
        auto success = params->aggregator.mergeOnBlock(block, variants, no_more_keys);
        return {!success, false};
    }
    else
        return params->aggregator.executeOnBlock(std::move(columns), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys);
}

void AggregatingTransform::emitVersion(Block & block)
{
    Int64 version = many_data->version++;
    block.insert(
        {params->version_type->createColumnConst(block.rows(), version)->convertToFullColumnIfConst(),
         params->version_type,
         ProtonConsts::RESERVED_EMIT_VERSION});
}

void AggregatingTransform::setCurrentChunk(Chunk chunk, const ChunkContextPtr & chunk_ctx)
{
    if (has_input)
        throw Exception("Current chunk was already set.", ErrorCodes::LOGICAL_ERROR);

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (chunk_ctx)
    {
        /// Aggregation is a stop operator, down stream will and shall establish its own watermark
        /// according to a new timestamp column emitted by the aggregator transform, so we will
        /// need clear watermark here and then propagate the chunk context to down stream.
        /// Then downstream will not be confused with upstream (processed) watermark
        /// FIXME, for global aggregation, we rely on periodic upstream timer to emit watermark,
        /// so we should not clear its watermark (watermark act as a timer). This shall be fixed
        /// by install a timer in down stream of the global aggregation instead of relying on
        /// downstream watermark as timer
        if (params->final && params->params.group_by != Aggregator::Params::GroupBy::OTHER)
            chunk_ctx->clearWatermark();

        current_chunk_aggregated.setChunkContext(std::move(chunk_ctx));
    }
}

IProcessor::Status AggregatingTransform::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}

void AggregatingTransform::checkpointAlignment(Chunk & chunk)
{
    auto ckpt_ctx = chunk.getCheckpointContext();

    ckpt_epoch = ckpt_ctx->epoch;

    if (many_data->ckpt_requested.fetch_add(1) + 1 == many_data->variants.size())
    {
        /// Validate all ckpt epochs are the same
        auto same_epochs
            = std::all_of(many_data->ckpt_epochs.begin(), many_data->ckpt_epochs.end(), [this](auto epoch) { return epoch == ckpt_epoch; });
        if (!same_epochs)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required same checkpoint epochs, but seen different ones");

        checkpoint(std::move(ckpt_ctx));

        many_data->ckpt_requested.store(0);
        many_data->ckpted.notify_all();

        /// Propagate the checkpoint barrier to all down stream output ports
        for (auto * transform : many_data->aggregating_transforms)
            transform->setCurrentChunk(Chunk{getOutputs().front().getHeader().getColumns(), 0}, chunk.getChunkContext());
    }
    else
    {
        /// Condition wait for last aggr thread to finish the checkpoint
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->ckpt_mutex);
        many_data->ckpted.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(log, "Took {} milliseconds to wait for checkpointing {} shard aggregation", end - start, many_data->variants.size());
    }
}

void AggregatingTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    /// FIXME, concurrency
    UInt16 num_variants = many_data->variants.size();
    Int64 last_version = many_data->version;
    for (size_t current_aggr = 0; const auto & data_variant : many_data->variants)
    {
        auto logic_id = many_data->aggregating_transforms[current_aggr]->getLogicID();
        UInt64 last_rows = *many_data->rows_since_last_finalizations[current_aggr];
        ckpt_ctx->coordinator->checkpoint(getVersion(), logic_id, ckpt_ctx, [num_variants, last_version, last_rows, data_variant, this](WriteBuffer & wb) {
            DB::writeIntBinary(num_variants, wb);
            DB::writeIntBinary(last_version, wb);
            DB::writeIntBinary(last_rows, wb);
            params->aggregator.checkpoint(*data_variant, wb);
        });

        ++current_aggr;
    }
}

void AggregatingTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    /// FIXME, concurrency
    for (size_t current_aggr = 0; auto & data_variant : many_data->variants)
    {
        auto logic_id = many_data->aggregating_transforms[current_aggr]->getLogicID();
        ckpt_ctx->coordinator->recover(logic_id, ckpt_ctx, [&data_variant, current_aggr, this](VersionType /*version*/, ReadBuffer & rb) {
            UInt16 num_variants = 0;
            DB::readIntBinary(num_variants, rb);
            if (num_variants != many_data->variants.size())
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover aggregation checkpoint. Number of data variants are not the same, checkpointed={}, current={}",
                    num_variants,
                    many_data->variants.size());

            Int64 last_version = 0;
            DB::readIntBinary(last_version, rb);
            many_data->version = last_version;

            UInt64 last_rows = 0;
            DB::readIntBinary(last_rows, rb);
            *many_data->rows_since_last_finalizations[current_aggr] = last_rows;

            params->aggregator.recover(*data_variant, rb);
        });

        ++current_aggr;
    }
}

}
}
