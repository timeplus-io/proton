#include "StreamingAggregatingTransform.h"

#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <QueryPipeline/Pipe.h>

/// proton: starts. Added by proton

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
        AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, SharedDataPtr shared_data_, Arena * arena_)
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
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
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
        AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_)
        : IProcessor({}, {params_->getHeader()}), params(std::move(params_)), data(std::move(data_)), num_threads(num_threads_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksTransform"; }

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

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
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

        AggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator.mergeWithoutKeyDataImpl(*data);
            auto block = params->aggregator.prepareBlockAndFillWithoutKey(
                *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

            setCurrentChunk(convertToChunk(block));
        }
    }

    void mergeSingleLevel()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (current_bucket_num > 0 || first->type == AggregatedDataVariants::Type::without_key)
        {
            finished = true;
            return;
        }

        ++current_bucket_num;

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
        if (false)
        {
        } // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
        else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

        auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);

        setCurrentChunk(convertToChunk(block));
        finished = true;
    }

    void createSources()
    {
        AggregatedDataVariantsPtr & first = data->at(0);
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

StreamingAggregatingTransform::StreamingAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : StreamingAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

StreamingAggregatingTransform::StreamingAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
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
    if (is_consume_finished)
        initGenerate();
    else
    {
        if (finalizing)
        {
            /// In `finalizing` state, we can't aggregate the current chunk
            /// in memory because of potential race condition: there is another
            /// thread which tries to project an intermediate aggregation result
            /// for all in-memory aggr states
            return;
        }
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
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
        src_rows += num_rows;
        src_bytes += chunk.bytes();
        if (params->only_merge)
        {
            auto block = getInputs().front().getHeader().cloneWithColumns(chunk.detachColumns());
            block = materializeBlock(block);
            if (!params->aggregator.mergeOnBlock(block, variants, no_more_keys))
                is_consume_finished = true;
        }
        else
        {
            if (!params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys))
                is_consume_finished = true;
        }
    }

    if (needsFinalization(chunk))
    {
        finalize(chunk.getChunkInfo()->watermark);
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
    params->aggregator.params.streaming = false;

    if (!params->aggregator.hasTemporaryFiles())
    {
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
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

        addMergingAggregatedMemoryEfficientTransform(pipe, params, temporary_data_merge_threads);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

bool StreamingAggregatingTransform::needsFinalization(const Chunk & chunk) const
{
    /// FIXME, watermark alignment with other parallel aggr streams
    const auto & chunk_info = chunk.getChunkInfo();
    if (chunk_info && chunk_info->watermark != 0)
    {
        return true;
    }
    return false;
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
/// Only for streaming aggregation case
void StreamingAggregatingTransform::finalize(Int64 watermark)
{
    if (!finalizing)
    {
        finalizing = true;

        if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
        {
            /// The current transform is the last one in this round of
            /// finalization
            doFinalize(watermark);

            // Clear the finalization count
            many_data->finalizations.store(0);
            finalizing = false;

            return;
        }
    }

    /// FIXME: Wait for the last aggregation stream to do aggregation finalization
    /// condition wait or spin ?
}

void StreamingAggregatingTransform::doFinalize(Int64 watermark)
{
    /// FIXME spill to disk, aggr without key, overflow_row etc cases
    auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));

    if (prepared_data_ptr->empty())
        return;

    initialize(prepared_data_ptr);

    if (prepared_data_ptr->at(0)->isTwoLevel())
    {
        mergeTwoLevel(prepared_data_ptr, watermark);
    }
    else
    {
        mergeSingleLevel(prepared_data_ptr);
    }

    rows_since_last_finalization = 0;
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::initialize
void StreamingAggregatingTransform::initialize(ManyAggregatedDataVariantsPtr & data)
{
    assert(params->params.streaming);

    AggregatedDataVariantsPtr & first = data->at(0);

    /// At least we need one arena in first data item per thread
    if (max_threads > first->aggregates_pools.size())
    {
        Arenas & first_pool = first->aggregates_pools;
        for (size_t j = first_pool.size(); j < max_threads; j++)
            first_pool.emplace_back(std::make_shared<Arena>());
    }

    if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
    {
        params->aggregator.mergeWithoutKeyDataImpl(*data);
        auto block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

        setCurrentChunk(convertToChunk(block));
    }
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::mergeSingleLevel
void StreamingAggregatingTransform::mergeSingleLevel(ManyAggregatedDataVariantsPtr & data)
{
    assert(params->params.streaming);

    AggregatedDataVariantsPtr & first = data->at(0);

    if (first->type == AggregatedDataVariants::Type::without_key)
    {
        return;
    }

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);

    setCurrentChunk(convertToChunk(block));
}

void StreamingAggregatingTransform::mergeTwoLevel(ManyAggregatedDataVariantsPtr & data, Int64 watermark)
{
    /// FIXME, parallelization
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;

    for (size_t thread = 0; thread < max_threads; ++thread)
    {
        Arena * arena = first->aggregates_pools.at(thread).get();

        /// Figure out which buckets need get merged
        auto & data_variant = data->at(thread);
        std::vector<size_t> buckets = data_variant->aggregator->bucketsBefore(*data_variant, watermark);

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
        setCurrentChunk(convertToChunk(merged_block));

    auto [removed, remaining] = variants.aggregator->removeBucketsBefore(variants, watermark);
    LOG_INFO(
        log,
        "StreamingAggregated. removed {} windows less or equal to watermark={}, keeping window_count={}, remaining_windows={}",
        removed,
        watermark,
        params->aggregator.params.streaming_window_count,
        remaining);
}

void StreamingAggregatingTransform::setCurrentChunk(Chunk chunk)
{
    if (has_input)
        throw Exception("Current chunk was already set in StreamingAggregatingTransform.", ErrorCodes::LOGICAL_ERROR);

    has_input = true;
    current_chunk_aggregated = std::move(chunk);
}

IProcessor::Status StreamingAggregatingTransform::preparePushToOutput()
{
    auto & output = outputs.front();
    output.push(std::move(current_chunk_aggregated));
    has_input = false;

    return Status::PortFull;
}
}
