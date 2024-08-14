#include <Processors/Transforms/AggregatingTransform.h>

#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Core/ProtocolDefines.h>

/// proton: starts
#include <Processors/Transforms/convertToChunk.h>

#include <ranges>
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
    class SourceFromNativeStream final : public ISource
    {
    public:
        SourceFromNativeStream(const Block & header, const std::string & path)
                : ISource(header, true, ProcessorID::SourceFromNativeStreamID), file_in(path), compressed_in(file_in),
                  block_in(std::make_unique<NativeReader>(compressed_in, DBMS_TCP_PROTOCOL_VERSION))
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
class ConvertingAggregatedToChunksSource final : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed;
        std::atomic<bool> is_cancelled = false;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksSource(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        Arena * arena_)
        : ISource(params_->getHeader(), false, ProcessorID::ConvertingAggregatedToChunksSourceID)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
        {}

    String getName() const override { return "ConvertingAggregatedToChunksSource"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
            return {};

        Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket_num, &shared_data->is_cancelled);
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

/// proton : starts
class ConvertingAggregatedToChunksSourceShuffled final : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::vector<std::unique_ptr<std::atomic<bool>>> is_bucket_processed;

        void assertAllBucketsAreProcessed() const
        {
            if (std::ranges::any_of(is_bucket_processed, [](const auto & processed) { return !processed->load(); }))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Some aggregate buckets are not processed before finish");
        }

        explicit SharedData(size_t num_data_variants)
        {
            for (size_t i = 0; i < num_data_variants * NUM_BUCKETS; ++i)
                is_bucket_processed.push_back(std::make_unique<std::atomic<bool>>(false));
        }
    };
    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksSourceShuffled(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        Arena * arena_)
        : ISource(params_->getHeader(), false, ProcessorID::ConvertingAggregatedToChunksSourceShuffledID)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksSourceShuffled"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= shared_data->is_bucket_processed.size())
            return {};

        auto data_variants_index = bucket_num / NUM_BUCKETS;
        auto & data_variant = data->at(data_variants_index);
        auto data_variants_bucket = static_cast<Int32>(bucket_num % NUM_BUCKETS);
        Block block = params->aggregator.convertOneBucketToBlock(*data_variant, arena, params->final, data_variants_bucket);
        Chunk chunk = convertToChunk(block);

        *shared_data->is_bucket_processed[bucket_num] = true;

        return chunk;
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
};
/// proton : ends

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksSource` workers:
///
/// ConvertingAggregatedToChunksSource ->
/// ConvertingAggregatedToChunksSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class ConvertingAggregatedToChunksTransform final : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransform(AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_, bool shuffled_)
        : IProcessor({}, {params_->getHeader()}, ProcessorID::ConvertingAggregatedToChunksTransformID)
        , params(std::move(params_)), data(std::move(data_)), num_threads(num_threads_), shuffled(shuffled_) {}

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

        if (finished && single_level_chunks.empty())
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

        if (!single_level_chunks.empty())
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
        if (single_level_chunks.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Some ready chunks expected");

        auto & output = outputs.front();
        output.push(std::move(single_level_chunks.back()));
        single_level_chunks.pop_back();

        if (finished && single_level_chunks.empty())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// Read all sources and try to push current bucket.
    IProcessor::Status prepareTwoLevel()
    {
        if (shuffled)
            return prepareTwoLevelShuffled();

        auto & output = outputs.front();

        for (auto & input : inputs)
        {
            if (!input.isFinished() && input.hasData())
            {
                auto chunk = input.pull();
                auto bucket = getInfoFromChunk(chunk)->bucket_num;
                two_level_chunks[bucket] = std::move(chunk);
            }
        }

        if (!shared_data->is_bucket_processed[current_bucket_num])
            return Status::NeedData;

        if (!two_level_chunks[current_bucket_num])
            return Status::NeedData;

        output.push(std::move(two_level_chunks[current_bucket_num]));

        ++current_bucket_num;
        if (current_bucket_num == NUM_BUCKETS)
        {
            output.finish();
            /// Do not close inputs, they must be finished.
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// proton : starts
    /// Read all sources and try to push current processed bucket.
    IProcessor::Status prepareTwoLevelShuffled()
    {
        auto & output = outputs.front();

        size_t new_processed = 0;
        for (auto & input : inputs)
        {
            if (!input.isFinished() && input.hasData())
            {
                shuffled_chunks.push_back(input.pull());
                ++new_processed;
            }
        }
        num_buckets_processed += new_processed;

        if (shuffled_chunks.empty())
            return Status::NeedData;

        output.push(std::move(shuffled_chunks.front()));
        shuffled_chunks.pop_front();

        if ((num_buckets_processed == shuffled_shared_data->is_bucket_processed.size()) && shuffled_chunks.empty())
        {
            shuffled_shared_data->assertAllBucketsAreProcessed();

            output.finish();
            /// Do not close inputs, they must be finished.
            return Status::Finished;
        }

        return Status::PortFull;
    }
    /// proton : ends

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    ConvertingAggregatedToChunksSource::SharedDataPtr shared_data;
    ConvertingAggregatedToChunksSourceShuffled::SharedDataPtr shuffled_shared_data;

    size_t num_threads;

    bool is_initialized = false;
    bool finished = false;

    /// proton : starts
    /// When input is shuffled already, there is no key overlap among data variants,
    /// so the merge process can be skipped
    bool shuffled;

    /// Next data variants to convert to Block.
    /// For single level conversion
    size_t next_data_variants = 0;

    /// For two level conversion
    size_t num_buckets_processed = 0;
    ChunkList shuffled_chunks;
    /// proton : ends

    Chunks single_level_chunks;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> two_level_chunks;

    Processors processors;

    void initialize()
    {
        is_initialized = true;

        AggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; ++j)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator.mergeWithoutKeyDataImpl(*data);
            auto block = params->aggregator.prepareBlockAndFillWithoutKey(
                *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

            single_level_chunks.emplace_back(convertToChunk(block));
        }
    }

    void mergeSingleLevel()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (first->type == AggregatedDataVariants::Type::without_key)
        {
            finished = true;
            return;
        }

        if (!shuffled)
        {
            next_data_variants = data->size();

#define M(NAME) \
                else if (first->type == AggregatedDataVariants::Type::NAME) \
                    params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
            if (false)
            {
            } // NOLINT
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
            else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

            auto blocks = params->aggregator.prepareBlockAndFillSingleLevel</* return_single_block */ false>(*first, params->final);
            for (auto & block : blocks)
                single_level_chunks.emplace_back(convertToChunk(block));

            finished = true;
        }
        else
        {
            /// Convert one data variant at a time to a block for shuffled case
            auto next = data->at(next_data_variants);
            auto blocks = params->aggregator.prepareBlockAndFillSingleLevel</* return_single_block */ false>(*next, params->final);
            for (auto & block : blocks)
                single_level_chunks.emplace_back(convertToChunk(block));

            ++next_data_variants;

            if (next_data_variants >= data->size())
                finished = true;
        }
    }

    void createSources()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (shuffled)
            shuffled_shared_data = std::make_shared<ConvertingAggregatedToChunksSourceShuffled::SharedData>(data->size());
        else
            shared_data = std::make_shared<ConvertingAggregatedToChunksSource::SharedData>();

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            /// Select Arena to avoid race conditions
            Arena * arena = first->aggregates_pools.at(thread).get();
            ProcessorPtr source = nullptr;
            if (shuffled)
                source = std::make_shared<ConvertingAggregatedToChunksSourceShuffled>(params, data, shuffled_shared_data, arena);
            else
                source = std::make_shared<ConvertingAggregatedToChunksSource>(params, data, shared_data, arena);

            processors.emplace_back(std::move(source));
        }
    }
};

AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransform(std::move(header), std::move(params_)
    , std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()}, ProcessorID::AggregatingTransformID)
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , current_variant(current_variant_)
    , variants(*many_data->variants[current_variant_])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
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

void AggregatingTransform::work()
{
    if (is_consume_finished)
        initGenerate();
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
}

Processors AggregatingTransform::expandPipeline()
{
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
}

void AggregatingTransform::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating");
        is_consume_started = true;
    }

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
        if (!params->aggregator.executeOnBlock(chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys))
            is_consume_finished = true;
    }
}

void AggregatingTransform::initGenerate()
{
    if (is_generate_initialized)
        return;

    is_generate_initialized = true;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
    {
        if (params->only_merge)
            params->aggregator.mergeOnBlock(getInputs().front().getHeader(), variants, no_more_keys);
        else
            params->aggregator.executeOnBlock(getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();

    LOG_INFO(log, "Aggregated. current_variant={} {} to {} rows (from {}) in {} sec. ({:.3f} rows/sec., {}/sec.)",
        current_variant,
        src_rows, rows, ReadableSize(src_bytes),
        elapsed_seconds, src_rows / elapsed_seconds,
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

    if (!params->aggregator.hasTemporaryFiles())
    {
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
        processors.emplace_back(std::make_shared<ConvertingAggregatedToChunksTransform>(params, std::move(prepared_data_ptr), max_threads, params->shuffled));
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

        LOG_INFO(
            log,
            "Will merge {} temporary files of size {} compressed, {} uncompressed.",
            files.files.size(),
            ReadableSize(files.sum_size_compressed),
            ReadableSize(files.sum_size_uncompressed));

        addMergingAggregatedMemoryEfficientTransform(pipe, params, temporary_data_merge_threads);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

}
