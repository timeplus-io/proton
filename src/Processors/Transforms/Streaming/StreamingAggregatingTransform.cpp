#include "StreamingAggregatingTransform.h"

#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/Pipe.h>

/// proton: starts. Added by proton
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Streaming/SessionMap.h>
#include <Common/ProtonCommon.h>
/// proton: ends

namespace ProfileEvents
{
extern const Event ExternalAggregationMerge;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    const AggregatedChunkInfo * getInfoFromChunk(const Chunk & chunk)
    {
        const auto & info = chunk.getChunkInfo();
        if (!info)
            throw Exception("Chunk info was not set for chunk.", ErrorCodes::LOGICAL_ERROR);

        const auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
        if (!agg_info)
            throw Exception("Chunk should have AggregatedChunkInfo.", ErrorCodes::LOGICAL_ERROR);

        return agg_info;
    }

    /// Reads chunks from file in native format. Provide chunks with aggregation info.
    class SourceFromNativeStream : public ISource
    {
    public:
        SourceFromNativeStream(const Block & header, const std::string & path)
            : ISource(header)
            , file_in(path)
            , compressed_in(file_in)
            , block_in(std::make_unique<NativeReader>(compressed_in, DBMS_TCP_PROTOCOL_VERSION))
        {
        }

        String getName() const override { return "SourceFromNativeStream"; }

        Chunk generate() override
        {
            if (!block_in)
                return {};

            auto block = block_in->read();
            if (!block)
            {
                block_in.reset();
                return {};
            }

            return convertToChunk(block);
        }

    private:
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        std::unique_ptr<NativeReader> block_in;
    };

    /// proton: starts.
    void splitColumns(size_t start, size_t size, IColumn::Filter & filt, size_t late_events, Columns & columns, Columns & to_process)
    {
        assert(columns.size() == to_process.size());
        for (size_t i = 0; i < columns.size(); i++)
        {
            to_process[i] = columns[i]->cut(start, size)->filter(filt, size - late_events);
        }
    }
    /// proton: ends
}

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class StreamingConvertingAggregatedToChunksSource : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed{};
        std::atomic<bool> is_cancelled = false;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    StreamingConvertingAggregatedToChunksSource(
        StreamingAggregatingTransformParamsPtr params_,
        ManyStreamingAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        Arena * arena_)
        : ISource(params_->getHeader())
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
    {
    }

    String getName() const override { return "StreamingConvertingAggregatedToChunksSource"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
            return {};

        Block block
            = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket_num, &shared_data->is_cancelled);
        Chunk chunk = convertToChunk(block);

        shared_data->is_bucket_processed[bucket_num] = true;

        return chunk;
    }

private:
    StreamingAggregatingTransformParamsPtr params;
    ManyStreamingAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
};

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `StreamingConvertingAggregatedToChunksSource` workers:
///
/// StreamingConvertingAggregatedToChunksSource ->
/// StreamingConvertingAggregatedToChunksSource -> StreamingConvertingAggregatedToChunksTransform -> AggregatingTransform
/// StreamingConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class StreamingConvertingAggregatedToChunksTransform : public IProcessor
{
public:
    StreamingConvertingAggregatedToChunksTransform(
        StreamingAggregatingTransformParamsPtr params_, ManyStreamingAggregatedDataVariantsPtr data_, size_t num_threads_)
        : IProcessor({}, {params_->getHeader()}), params(std::move(params_)), data(std::move(data_)), num_threads(num_threads_)
    {
    }

    String getName() const override { return "StreamingConvertingAggregatedToChunksTransform"; }

    void work() override
    {
        if (data->empty())
        {
            finished = true;
            return;
        }

        if (!is_initialized)
        {
            initialize();
            return;
        }

        /// proton: starts.
        Int64 start_ns = MonotonicNanoseconds::now();
        /// proton: ends.
        if (data->at(0)->isTwoLevel())
        {
            /// In two-level case will only create sources.
            if (inputs.empty())
                createSources();
        }
        else
        {
            mergeSingleLevel();
        }
        /// proton: starts.
        metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
        /// proton: ends.
    }

    Processors expandPipeline() override
    {
        for (auto & source : processors)
        {
            auto & out = source->getOutputs().front();
            inputs.emplace_back(out.getHeader(), this);
            connect(out, inputs.back());
            inputs.back().setNeeded();
        }

        return std::move(processors);
    }

    IProcessor::Status prepare() override
    {
        auto & output = outputs.front();

        if (finished && !has_input)
        {
            output.finish();
            return Status::Finished;
        }

        /// Check can output.
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            if (shared_data)
                shared_data->is_cancelled.store(true);

            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (!is_initialized)
            return Status::Ready;

        if (!processors.empty())
            return Status::ExpandPipeline;

        if (has_input)
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;

        /// Two-level case.
        return prepareTwoLevel();
    }

private:
    IProcessor::Status preparePushToOutput()
    {
        auto & output = outputs.front();
        output.push(std::move(current_chunk));
        has_input = false;

        if (finished)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// Read all sources and try to push current bucket.
    IProcessor::Status prepareTwoLevel()
    {
        auto & output = outputs.front();

        for (auto & input : inputs)
        {
            if (!input.isFinished() && input.hasData())
            {
                auto chunk = input.pull();
                auto bucket = getInfoFromChunk(chunk)->bucket_num;
                chunks[bucket] = std::move(chunk);
            }
        }

        if (!shared_data->is_bucket_processed[current_bucket_num])
            return Status::NeedData;

        if (!chunks[current_bucket_num])
            return Status::NeedData;

        output.push(std::move(chunks[current_bucket_num]));

        ++current_bucket_num;
        if (current_bucket_num == NUM_BUCKETS)
        {
            output.finish();
            /// Do not close inputs, they must be finished.
            return Status::Finished;
        }

        return Status::PortFull;
    }

    StreamingAggregatingTransformParamsPtr params;
    ManyStreamingAggregatedDataVariantsPtr data;
    StreamingConvertingAggregatedToChunksSource::SharedDataPtr shared_data;

    size_t num_threads;

    bool is_initialized = false;
    bool has_input = false;
    bool finished = false;

    Chunk current_chunk;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> chunks;

    Processors processors;

    void setCurrentChunk(Chunk chunk)
    {
        if (has_input)
            throw Exception(
                "Current chunk was already set in "
                "StreamingConvertingAggregatedToChunksTransform.",
                ErrorCodes::LOGICAL_ERROR);

        has_input = true;
        current_chunk = std::move(chunk);
    }

    void initialize()
    {
        is_initialized = true;

        StreamingAggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == StreamingAggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator.mergeWithoutKeyDataImpl(*data);
            auto block = params->aggregator.prepareBlockAndFillWithoutKey(
                *first, params->final, first->type != StreamingAggregatedDataVariants::Type::without_key);

            setCurrentChunk(convertToChunk(block));
        }
    }

    void mergeSingleLevel()
    {
        StreamingAggregatedDataVariantsPtr & first = data->at(0);

        if (current_bucket_num > 0 || first->type == StreamingAggregatedDataVariants::Type::without_key)
        {
            finished = true;
            return;
        }

        ++current_bucket_num;

#define M(NAME) \
    else if (first->type == StreamingAggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
        if (false)
        {
        } // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
        else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

        auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);

        setCurrentChunk(convertToChunk(block));
        finished = true;
    }

    void createSources()
    {
        StreamingAggregatedDataVariantsPtr & first = data->at(0);
        shared_data = std::make_shared<StreamingConvertingAggregatedToChunksSource::SharedData>();

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            /// Select Arena to avoid race conditions
            Arena * arena = first->aggregates_pools.at(thread).get();
            auto source = std::make_shared<StreamingConvertingAggregatedToChunksSource>(params, data, shared_data, arena);

            processors.emplace_back(std::move(source));
        }
    }
};

StreamingAggregatingTransform::StreamingAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_)
    : StreamingAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyStreamingAggregatedData>(1), 0, 1, 1)
{
}

StreamingAggregatingTransform::StreamingAggregatingTransform(
    Block header,
    StreamingAggregatingTransformParamsPtr params_,
    ManyStreamingAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , watermark_bound(many_data->watermarks[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
    (void)temporary_data_merge_threads;
}

StreamingAggregatingTransform::~StreamingAggregatingTransform() = default;

IProcessor::Status StreamingAggregatingTransform::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Finish data processing, prepare to generating.
    if (is_consume_finished && !is_generate_initialized)
    {
        /// Close input port in case max_rows_to_group_by was reached but not all data was read.
        inputs.front().close();

        return Status::Ready;
    }

    if (is_generate_initialized && !is_pipeline_created && !processors.empty())
        return Status::ExpandPipeline;

    if (has_input)
        return preparePushToOutput();

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        if (is_consume_finished)
        {
            output.finish();
            return Status::Finished;
        }
        else
        {
            /// Finish data processing and create another pipe.
            is_consume_finished = true;
            return Status::Ready;
        }
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    if (is_consume_finished)
        input.setNeeded();

    current_chunk = input.pull(/*set_not_needed = */ !is_consume_finished);
    read_current_chunk = true;

    if (is_consume_finished)
    {
        output.push(std::move(current_chunk));
        read_current_chunk = false;
        return Status::PortFull;
    }

    return Status::Ready;
}

void StreamingAggregatingTransform::work()
{
    /// proton: starts.
    Int64 start_ns = MonotonicNanoseconds::now();
    metrics.processed_bytes += current_chunk.bytes();
    /// proton: ends.

    if (is_consume_finished)
        initGenerate();
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }

    /// proton: starts.
    metrics.processing_time_ns += MonotonicNanoseconds::now() - start_ns;
    /// proton: ends.
}

Processors StreamingAggregatingTransform::expandPipeline()
{
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
}

void StreamingAggregatingTransform::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    rows_since_last_finalization += num_rows;
    if (num_rows > 0)
    {
        Columns columns = chunk.detachColumns();
        /// proton: starts. for session window
        if(params->params.group_by == StreamingAggregator::Params::GroupBy::SESSION)
        {
            return consumeForSession(columns);
        }
        /// proton: ends.

        src_rows += num_rows;
        src_bytes += chunk.bytes();
        if (params->only_merge)
        {
            /// proton: starts
            auto block = getInputs().front().getHeader().cloneWithColumns(columns);
            /// proton: ends
            block = materializeBlock(block);
            if (!params->aggregator.mergeOnBlock(block, variants, no_more_keys))
                is_consume_finished = true;
        }
        else
        {
            /// proton: starts
            if (!params->aggregator.executeOnBlock(columns, num_rows, variants, key_columns, aggregate_columns, no_more_keys))
            /// proton: ends
                is_consume_finished = true;
        }
    }

    if (needsFinalization(chunk) && params->params.group_by != StreamingAggregator::Params::GroupBy::SESSION)
    {
        finalize(chunk.getChunkInfo());
    }
}

bool StreamingAggregatingTransform::executeOrMergeColumns(Columns & columns)
{
    size_t num_rows = columns[0]->size();
    src_rows += num_rows;
    for (const auto & column : columns)
        src_bytes += column->byteSize();

    if (params->only_merge)
    {
        /// proton: starts
        auto block = getInputs().front().getHeader().cloneWithColumns(columns);
        /// proton: ends
        block = materializeBlock(block);
        return params->aggregator.mergeOnBlock(block, variants, no_more_keys);
    }
    else
        return params->aggregator.executeOnBlock(columns, num_rows, variants, key_columns, aggregate_columns, no_more_keys);
}

void StreamingAggregatingTransform::consumeForSession(Columns & columns)
{
    size_t num_rows = columns[0]->size();
    ColumnPtr session_id_column;
    Block merged_block;

    /// Get session_id column
    session_id_column = columns.at(params->params.keys[0]);

    /// proton: starts. Prepare for session window
    ColumnPtr time_column = columns.at(params->params.time_col_pos);

    size_t prev = 0;
    size_t late_events = 0;
    IColumn::Filter filter(num_rows, 1);

    for (size_t i = 0; i < num_rows;)
    {
        SessionStatus status;
        /// filter starts from prev to pos of event to be emitted
        filter.resize_fill(num_rows - prev, 1);

        /// For session window, it has two rounds to execute each row of block if it might trigger a session emit.
        /// the first round is to find all sessions to emit, then emit those sessions before process the current row.
        /// After emit sessions and clear up session info, the second round to process the current row.
        if (params->params.time_col_is_datetime64)
        {
            status = params->aggregator.processSessionRow<ColumnDecimal<DateTime64>>(
                const_cast<SessionHashMap &>(params->aggregator.session_map),
                session_id_column,
                time_column,
                i,
                const_cast<Int64 &>(params->aggregator.max_event_ts));
        }
        else
        {
            status = params->aggregator.processSessionRow<ColumnVector<UInt32>>(
                const_cast<SessionHashMap &>(params->aggregator.session_map),
                session_id_column,
                time_column,
                i,
                const_cast<Int64 &>(params->aggregator.max_event_ts));
        }

        if (status != SessionStatus::IGNORE)
        {
            filter[i - prev] = 1;
        }
        else
        {
            filter[i - prev] = 0;
            late_events++;
        }

        if (status == SessionStatus::EMIT)
        {
            /// if need to emit, first split rows before emit row to process,
            /// then finalizeSessio0 to emit sessions,
            /// last continue to process next row
            Columns to_process(columns.size());
            if (i > prev)
            {
                filter.resize_fill(i - prev);
                splitColumns(prev, i - prev, filter, late_events, columns, to_process);
                /// FIXME: handle the process error when return value is true
                if (!executeOrMergeColumns(to_process))
                    is_consume_finished = false;
                prev = i;
                late_events = 0;
            }

            finalizeSession(params->aggregator.sessions_to_emit, merged_block);
        }
        else
            i++;
    }

    if (prev < num_rows)
    {
        Columns to_process(columns.size());
        splitColumns(prev, num_rows - prev, filter, late_events, columns, to_process);

        /// FIXME: handle the process error when return value is true
        if (!executeOrMergeColumns(to_process))
            is_consume_finished = false;
    }

    if (merged_block.rows() > 0)
    {
        ChunkInfoPtr info = std::make_shared<ChunkInfo>();
        setCurrentChunk(convertToChunk(merged_block), info);
    }
}

void StreamingAggregatingTransform::initGenerate()
{
    if (is_generate_initialized)
        return;

    is_generate_initialized = true;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
    {
        if (params->only_merge) /// FIXME, only_merge case
            params->aggregator.mergeOnBlock(getInputs().front().getHeader(), variants, no_more_keys);
        else
            params->aggregator.executeOnBlock(getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();

    LOG_DEBUG(
        log,
        "StreamingAggregated. {} to {} rows (from {}) in {} sec. ({:.3f} rows/sec., {}/sec.)",
        src_rows,
        rows,
        ReadableSize(src_bytes),
        elapsed_seconds,
        src_rows / elapsed_seconds,
        ReadableSize(src_bytes / elapsed_seconds));

    if (params->aggregator.hasTemporaryFiles())
    {
        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();

        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        if (!variants.empty())
            params->aggregator.writeToTemporaryFile(variants);
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->variants.size())
        return;

    /// All aggregation streams are done
    /// Disable streaming mode to release all in-memory states since
    /// we are finalizing the aggregation result
    params->aggregator.params.keep_state = false;

    if (!params->aggregator.hasTemporaryFiles())
    {
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));
        processors.emplace_back(
            std::make_shared<StreamingConvertingAggregatedToChunksTransform>(params, std::move(prepared_data_ptr), max_threads));
    }
    else
    {
        /// If there are temporary files with partially-aggregated data on the disk,
        /// then read and merge them, spending the minimum amount of memory.

        ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

        if (many_data->variants.size() > 1)
        {
            /// It may happen that some data has not yet been flushed,
            ///  because at the time thread has finished, no data has been flushed to disk, and then some were.
            for (auto & cur_variants : many_data->variants)
            {
                if (cur_variants->isConvertibleToTwoLevel())
                    cur_variants->convertToTwoLevel();

                if (!cur_variants->empty())
                    params->aggregator.writeToTemporaryFile(*cur_variants);
            }
        }

        const auto & files = params->aggregator.getTemporaryFiles();
        Pipe pipe;

        {
            auto header = params->aggregator.getHeader(false);
            Pipes pipes;

            for (const auto & file : files.files)
                pipes.emplace_back(Pipe(std::make_unique<SourceFromNativeStream>(header, file->path())));

            pipe = Pipe::unitePipes(std::move(pipes));
        }

        LOG_DEBUG(
            log,
            "StreamingAggregator will merge {} temporary files of size {} compressed, {} uncompressed.",
            files.files.size(),
            ReadableSize(files.sum_size_compressed),
            ReadableSize(files.sum_size_uncompressed));

        /// FIXME
        /// addMergingAggregatedMemoryEfficientTransform(pipe, params, temporary_data_merge_threads);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

bool StreamingAggregatingTransform::needsFinalization(const Chunk & chunk) const
{
    const auto & chunk_info = chunk.getChunkInfo();
    if (chunk_info && chunk_info->ctx.hasWatermark() != 0)
    {
        return true;
    }
    return false;
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
/// Only for streaming aggregation case
void StreamingAggregatingTransform::finalize(ChunkInfoPtr chunk_info)
{
    chunk_info->ctx.getWatermark(watermark_bound.watermark, watermark_bound.watermark_lower_bound);

    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        /// The current transform is the last one in this round of
        /// finalization. Do watermark alignment for all of the variants
        /// pick the smallest watermark
        WatermarkBound min_watermark{watermark_bound};
        WatermarkBound max_watermark{watermark_bound};

        for (auto & bound : many_data->watermarks)
        {
            if (bound.watermark < min_watermark.watermark)
                min_watermark = bound;

            if (bound.watermark > min_watermark.watermark)
                max_watermark = bound;

            /// Reset watermarks
            bound.watermark = 0;
            bound.watermark_lower_bound = 0;
        }

        if (min_watermark.watermark != max_watermark.watermark)
            LOG_INFO(
                log,
                "StreamingAggregated. Found watermark skew. min_watermark={}, max_watermark={}",
                min_watermark.watermark,
                min_watermark.watermark);

        auto start = MonotonicMilliseconds::now();
        doFinalize(min_watermark, chunk_info);
        auto end = MonotonicMilliseconds::now();

        LOG_INFO(
            log, "StreamingAggregated. Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();

        /// We first notify all other variants that the aggregation is done for this round
        /// and then remove the project window buckets and their memory arena for the current variant.
        /// This save a bit time and a bit more efficiency because all variants can do memory arena
        /// recycling in parallel.
        removeBuckets();
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "StreamingAggregated. Took {} milliseconds to wait for finalizing {} shard aggregation",
            end - start,
            many_data->variants.size());

        removeBuckets();
    }
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
/// Only for streaming aggregation case
void StreamingAggregatingTransform::finalizeSession(std::vector<size_t> & sessions, Block & merged_block)
{
    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        auto start = MonotonicMilliseconds::now();

        /// FIXME spill to disk, overflow_row etc cases
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));

        if (prepared_data_ptr->empty())
            return;

        /// At least we need one arena in first data item per thread
        StreamingAggregatedDataVariantsPtr & first = prepared_data_ptr->at(0);
        if (max_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < max_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (prepared_data_ptr->at(0)->isTwoLevel())
        {
            mergeTwoLevelSessionWindow(prepared_data_ptr, sessions, merged_block);
        }

        rows_since_last_finalization = 0;


        auto end = MonotonicMilliseconds::now();

        LOG_INFO(
            log, "StreamingAggregated. Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();

        /// We first notify all other variants that the aggregation is done for this round
        /// and then remove the project window buckets and their memory arena for the current variant.
        /// This save a bit time and a bit more efficiency because all variants can do memory arena
        /// recycling in parallel.
        removeBucketsOfSessions(sessions);
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "StreamingAggregated. Took {} milliseconds to wait for finalizing {} shard aggregation",
            end - start,
            many_data->variants.size());

        removeBucketsOfSessions(sessions);
    }
}

void StreamingAggregatingTransform::doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));

    if (prepared_data_ptr->empty())
        return;

    initialize(prepared_data_ptr, chunk_info);

    if (prepared_data_ptr->at(0)->isTwoLevel())
    {
        if (params->params.group_by != StreamingAggregator::Params::GroupBy::OTHER)
            mergeTwoLevelStreamingWindow(prepared_data_ptr, watermark, chunk_info);
        else
            mergeTwoLevel(prepared_data_ptr, chunk_info);
    }
    else
    {
        mergeSingleLevel(prepared_data_ptr, watermark, chunk_info);
    }

    rows_since_last_finalization = 0;
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::initialize
void StreamingAggregatingTransform::initialize(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info)
{
    StreamingAggregatedDataVariantsPtr & first = data->at(0);

    /// At least we need one arena in first data item per thread
    if (max_threads > first->aggregates_pools.size())
    {
        Arenas & first_pool = first->aggregates_pools;
        for (size_t j = first_pool.size(); j < max_threads; j++)
            first_pool.emplace_back(std::make_shared<Arena>());
    }

    if (first->type == StreamingAggregatedDataVariants::Type::without_key || params->params.overflow_row)
    {
        params->aggregator.mergeWithoutKeyDataImpl(*data);
        auto block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != StreamingAggregatedDataVariants::Type::without_key);

        setCurrentChunk(convertToChunk(block), chunk_info);
    }
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::mergeSingleLevel
void StreamingAggregatingTransform::mergeSingleLevel(
    ManyStreamingAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info)
{
    StreamingAggregatedDataVariantsPtr & first = data->at(0);

    if (first->type == StreamingAggregatedDataVariants::Type::without_key)
    {
        return;
    }

#define M(NAME) \
    else if (first->type == StreamingAggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);
    /// Tell other variants to clean up memory arena
    many_data->arena_watermark = watermark;

    setCurrentChunk(convertToChunk(block), chunk_info);
}

void StreamingAggregatingTransform::mergeTwoLevel(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info)
{
    (void)data;
    (void)chunk_info;
}

void StreamingAggregatingTransform::mergeTwoLevelStreamingWindow(
    ManyStreamingAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;

    size_t index = data->size() == 1 ? 0 : 1;
    for (; index < first->aggregates_pools.size(); ++index)
    {
        Arena * arena = first->aggregates_pools.at(index).get();

        /// Figure out which buckets need get merged
        auto & data_variant = data->at(index);
        std::vector<size_t> buckets
            = data_variant->aggregator->bucketsBefore(*data_variant, watermark.watermark_lower_bound, watermark.watermark);

        for (auto bucket : buckets)
        {
            Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket, &is_cancelled);
            if (is_cancelled)
                return;

            if (merged_block)
            {
                assertBlocksHaveEqualStructure(merged_block, block, "merging buckets for streaming two level hashtable");
                for (size_t i = 0, size = merged_block.columns(); i < size; ++i)
                {
                    const auto source_column = block.getByPosition(i).column;
                    auto mutable_column = IColumn::mutate(std::move(merged_block.getByPosition(i).column));
                    mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                    merged_block.getByPosition(i).column = std::move(mutable_column);
                }
            }
            else
                merged_block = std::move(block);
        }
    }

    if (merged_block)
        setCurrentChunk(convertToChunk(merged_block), chunk_info);

    /// Tell other variants to clean up memory arena
    many_data->arena_watermark = watermark;
}

void StreamingAggregatingTransform::mergeTwoLevelSessionWindow(
    ManyStreamingAggregatedDataVariantsPtr & data, const std::vector<size_t> & sessions, Block & final_block)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;
    Block header = first->aggregator->getHeader(true, false).cloneEmpty();
    auto window_start_col = header.getByName(ProtonConsts::STREAMING_WINDOW_START);
    auto window_start_col_ptr = IColumn::mutate(window_start_col.column);
    auto window_end_col = header.getByName(ProtonConsts::STREAMING_WINDOW_END);
    auto window_end_col_ptr = IColumn::mutate(window_end_col.column);

    size_t index = data->size() == 1 ? 0 : 1;
    for (; index < first->aggregates_pools.size(); ++index)
    {
        Arena * arena = first->aggregates_pools.at(index).get();

        /// Figure out which buckets need get merged
        auto & data_variant = data->at(index);

        for (const size_t & session_id : sessions)
        {
            size_t session_rows = 0;

            SessionInfo & info = *(const_cast<SessionHashMap &>(data_variant->aggregator->session_map).getSessionInfo(session_id));
            LOG_DEBUG(log, "emit session {} with {}", info.id, info.toString());

            /// emit session
            std::vector<size_t> buckets = data_variant->aggregator->bucketsOfSession(*data_variant, info.id);

            for (auto bucket : buckets)
            {
                Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket, &is_cancelled);
                if (is_cancelled)
                    return;

                session_rows += block.rows();
                if (merged_block)
                {
                    assertBlocksHaveEqualStructure(merged_block, block, "merging buckets for streaming two level hashtable");
                    for (size_t i = 0, size = merged_block.columns(); i < size; ++i)
                    {
                        const auto source_column = block.getByPosition(i).column;
                        auto mutable_column = IColumn::mutate(std::move(merged_block.getByPosition(i).column));
                        mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                        merged_block.getByPosition(i).column = std::move(mutable_column);
                    }
                }
                else
                    merged_block = std::move(block);
            }

            if (merged_block)
            {
                /// fill session info columns, i.e. 'window_start', 'window_end'
                for (size_t i = 0; i < session_rows; i++)
                {
                    if (params->params.time_col_is_datetime64)
                    {
                        window_start_col_ptr->insert(
                            DecimalUtils::decimalFromComponents<DateTime64>(info.win_start, 0, params->params.time_scale));
                        window_end_col_ptr->insert(
                            DecimalUtils::decimalFromComponents<DateTime64>(info.win_end, 0, params->params.time_scale));
                    }
                    else
                    {
                        window_start_col_ptr->insert(info.win_start);
                        window_end_col_ptr->insert(info.win_end);
                    }
                }
            }
        }
    }
    LOG_DEBUG(log, "total {} sessions, emit {} sessions", params->aggregator.session_map.size(), sessions.size());

    if (merged_block && merged_block.rows() > 0)
    {
        merged_block.insert(1, {std::move(window_end_col_ptr), window_end_col.type, window_end_col.name});
        merged_block.insert(1, {std::move(window_start_col_ptr), window_start_col.type, window_start_col.name});
    }

    if (final_block.rows()>0)
    {
        assertBlocksHaveEqualStructure(merged_block, final_block, "merging buckets for streaming two level hashtable");
        for (size_t i = 0, size = final_block.columns(); i < size; ++i)
        {
            const auto source_column = merged_block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(final_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            final_block.getByPosition(i).column = std::move(mutable_column);
        }
    } else
        final_block = std::move(merged_block);
}

/// Cleanup memory arena for the projected window buckets
void StreamingAggregatingTransform::removeBuckets()
{
    if (params->params.group_by != StreamingAggregator::Params::GroupBy::OTHER)
        variants.aggregator->removeBucketsBefore(
            variants, many_data->arena_watermark.watermark_lower_bound, many_data->arena_watermark.watermark);
}

/// Cleanup memory arena for the projected window buckets
void StreamingAggregatingTransform::removeBucketsOfSessions(std::vector<size_t> & sessions)
{
    if (params->params.group_by == StreamingAggregator::Params::GroupBy::SESSION)
    {
        for (const size_t & session_id : sessions)
            variants.aggregator->removeBucketsOfSession(variants, session_id);
        const_cast<StreamingAggregator *>(variants.aggregator)->clearInfoOfEmitSessions();
    }
}

void StreamingAggregatingTransform::setCurrentChunk(Chunk chunk, ChunkInfoPtr & chunk_Info)
{
    if (has_input)
        throw Exception("Current chunk was already set in StreamingAggregatingTransform.", ErrorCodes::LOGICAL_ERROR);

    has_input = true;
    current_chunk_aggregated = std::move(chunk);

    if (params->params.group_by == StreamingAggregator::Params::GroupBy::OTHER)
        current_chunk_aggregated.setChunkInfo(std::move(chunk_Info));
}

IProcessor::Status StreamingAggregatingTransform::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}
}
