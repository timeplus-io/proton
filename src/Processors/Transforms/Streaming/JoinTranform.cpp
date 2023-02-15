#include <Processors/Transforms/Streaming/JoinTranform.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int TOO_MANY_BYTES;
}

namespace Streaming
{
Block JoinTransform::transformHeader(Block header, const HashJoinPtr & join)
{
    join->checkTypesOfKeys(header);
    ExtraBlockPtr tmp;
    join->joinBlock(header, tmp);

    /// FIXME, more general design for changelog emit
    if (join->emitChangeLog() && !header.has(ProtonConsts::RESERVED_DELTA_FLAG))
        header.insert({DataTypeFactory::instance().get("int8"), ProtonConsts::RESERVED_DELTA_FLAG});

    return header;
}

JoinTransform::JoinTransform(
    Block left_input_header,
    Block right_input_header,
    HashJoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_wait_ms_,
    UInt64 join_max_wait_rows_,
    UInt64 join_max_cached_bytes_,
    FinishCounterPtr finish_counter_)
    : IProcessor(
        {left_input_header, right_input_header}, {transformHeader(left_input_header, join_)}, ProcessorID::StreamingJoinTransformID)
    , insert_funcs({&HashJoin::insertLeftBlock, &HashJoin::insertRightBlock})
    , port_can_have_more_data{true, true}
    , output_header(outputs.front().getHeader())
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , join(std::move(join_))
    , finish_counter(std::move(finish_counter_))
    , max_block_size(max_block_size_)
    , join_max_wait_ms(join_max_wait_ms_)
    , join_max_wait_rows(join_max_wait_rows_)
    , join_max_cached_bytes(join_max_cached_bytes_)
{
    assert(join);

    /// Validate asof join column data type
    validateAsofJoinKey(left_input_header, right_input_header);

    port_contexts.reserve(2);
    port_contexts.emplace_back(&inputs.front());
    port_contexts.emplace_back(&inputs.back());

    last_join = MonotonicMilliseconds::now();
}

IProcessor::Status JoinTransform::prepare()
{
    std::scoped_lock lock(mutex);

    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        output.finish();

        for (auto & port_ctx : port_contexts)
            port_ctx.input_port->close();

        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & port_ctx : port_contexts)
            port_ctx.input_port->setNotNeeded();

        return Status::PortFull;
    }

    if (!output_chunks.empty())
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
        return Status::PortFull;
    }

    for (size_t i = 0; auto & port_ctx : port_contexts)
    {
        if (port_ctx.has_input)
        {
            return Status::Ready;
        }
        else if (port_ctx.input_port->isFinished())
        {
            output.finish();
            return Status::Finished;
        }
        else
        {
            /// If we have cached too much data for that stream already, don't try to pull more
            if (port_can_have_more_data[i])
                port_ctx.input_port->setNeeded();
        }
        ++i;
    }

    for (size_t i = 0; auto & port_ctx : port_contexts)
    {
        if (port_ctx.input_port->hasData() && port_can_have_more_data[i])
        {
            port_ctx.input_chunk = port_ctx.input_port->pull(true);
            port_ctx.has_input = true;
        }
        ++i;
    }

    for (auto & port_ctx : port_contexts)
    {
        if (port_ctx.has_input)
            return Status::Ready;
    }

    return Status::NeedData;
}

void JoinTransform::work()
{
    bool has_data = false;
    bool has_watermark = false;
    std::vector<Block> blocks(2, Block{});
    {
        std::scoped_lock lock(mutex);

        assert(port_contexts[0].has_input || port_contexts[1].has_input);

        for (size_t i = 0; auto & port_ctx : port_contexts)
        {
            if (port_ctx.has_input)
            {
                if (port_ctx.input_chunk.hasRows())
                {
                    added_rows_since_last_join += port_ctx.input_chunk.getNumRows();
                    auto block{port_ctx.input_port->getHeader().cloneWithColumns(port_ctx.input_chunk.detachColumns())};
                    blocks[i].swap(block);
                    has_data = true;
                }

                if (port_ctx.input_chunk.hasWatermark())
                {
                    /// We will use one of the watermark if both of them are present at port
                    /// FIXME
                    output_header_chunk.setChunkContext(port_ctx.input_chunk.getChunkContext());
                    has_watermark = true;
                }

                port_ctx.has_input = false;
            }

            ++i;
        }
    }

    if (!has_data)
    {
        if (has_watermark)
        {
            /// Pure watermark chunk, propagate it
            std::scoped_lock lock(mutex);
            output_chunks.emplace_back(output_header_chunk.clone());
            output_header_chunk.setChunkInfo(nullptr);
            output_header_chunk.setChunkContext(nullptr);
        }
        return;
    }

    if (join->emitChangeLog())
    {
        joinBidirectionally(std::move(blocks));
    }
    else if (!join->needBufferLeftStream())
    {
        /// SELECT * FROM append_only_stream JOIN versioned_kv ON append_only_stream.key=versioned_kv.key;

        /// First insert right block to update the build-side hash table
        if (blocks[1])
            join->insertRightBlock(std::move(blocks[1]));

        /// Then use left block to join the right updated hash table
        /// Please note in this mode, right stream data only changes won't trigger join since left stream data is not buffered
        if (blocks[0])
        {
            ExtraBlockPtr extra_block;
            join->joinBlock(blocks[0], extra_block);
            if (blocks[0])
            {
                std::scoped_lock lock(mutex);
                /// Piggy-back watermark in header's chunk info if there is
                output_chunks.emplace_back(
                    blocks[0].getColumns(), blocks[0].rows(), output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());
                output_header_chunk.setChunkInfo(nullptr);
                output_header_chunk.setChunkContext(nullptr);
            }
        }
    }
    else
    {
        bufferDataAndJoin(std::move(blocks));
    }
}

void JoinTransform::bufferDataAndJoin(std::vector<Block> && blocks)
{
    bool only_timer_blocks = true;

    /// FIXME, non_joined_blocks
    /// Doesn't have not processed block, normal case
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        if (!blocks[i])
            continue;

        only_timer_blocks = false;
        auto cached_bytes_so_far = std::invoke(insert_funcs[i], join.get(), std::move(blocks[i]));
        if (cached_bytes_so_far > join_max_cached_bytes)
            port_can_have_more_data[i] = false;
    }

    if (timeToJoin())
    {
        size_t left_cached_bytes = 0;
        size_t right_cached_bytes = 0;

        auto block{join->joinBlocks(left_cached_bytes, right_cached_bytes)};
        if (block)
        {
            std::scoped_lock lock(mutex);
            /// Piggy-back watermark in header's chunk info if there is
            output_chunks.emplace_back(
                block.getColumns(), block.rows(), output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());
            output_header_chunk.setChunkInfo(nullptr);
            output_header_chunk.setChunkContext(nullptr);
        }

        /// Join may trigger data recycling
        /// If cached bytes drop below threshold, clear the no more data flag
        port_can_have_more_data[0] = left_cached_bytes <= join_max_cached_bytes;
        port_can_have_more_data[1] = right_cached_bytes <= join_max_cached_bytes;
    }

    {
        std::scoped_lock lock(mutex);
        if (only_timer_blocks || output_header_chunk.hasWatermark())
        {
            /// We like to propagate the empty timer block as well
            /// FIXME, when high performance timer is ready, we probably don't need pass along the empty timer block
            /// in the pipeline
            output_chunks.emplace_back(output_header_chunk.clone());
            output_header_chunk.setChunkContext(nullptr);
        }
    }

    if (!port_can_have_more_data[0] && !port_can_have_more_data[1])
        /// Both stream are full of data, to avoid deadlock, throw
        throw Exception(
            ErrorCodes::TOO_MANY_BYTES, "There are too much data cached for both left stream and right stream in the stream join");
}

bool JoinTransform::timeToJoin() const
{
    std::scoped_lock lock(mutex);

    UInt64 now = MonotonicMilliseconds::now();
    if (now - last_join >= join_max_wait_ms || added_rows_since_last_join >= join_max_wait_rows)
    {
        /// We reset the tracking metrics here instead of `after join` to avoid race
        last_join = now;
        added_rows_since_last_join = 0;
        return true;
    }
    return false;
}

void JoinTransform::joinBidirectionally(std::vector<Block> && blocks)
{
    /// We need buffer left and right stream data
    /// And we also join each block every time
    /// First add new blocks
    auto & left_block = blocks[0];
    auto & right_block = blocks[1];
    auto left_block_rows = left_block.rows();
    auto right_block_rows = right_block.rows();

    assert (left_block_rows || right_block_rows);

    if (left_block_rows)
        join->insertLeftBlock(left_block); /// We like to copy the block over since we need join it later

    if (right_block_rows)
        join->insertRightBlock(right_block);

    /// Then join
    std::vector<Block> retracted_blocks(2, Block{});
    if (left_block_rows)
        retracted_blocks[0] = join->joinWithRightBlocks(left_block);

    if (right_block_rows)
        retracted_blocks[1] = join->joinWithLeftBlocks(right_block, output_header);

    {
        std::scoped_lock lock(mutex);
        for (size_t i = 0; auto & block : blocks)
        {
            auto block_rows = block.rows();
            if (block_rows)
            {
                /// First emit retracted block
                if (retracted_blocks[i].rows())
                {
                    /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
                    auto chunk_ctx = std::make_shared<ChunkContext>();
                    chunk_ctx->setAvoidWatermark();
                    output_chunks.emplace_back(retracted_blocks[i].getColumns(), retracted_blocks[i].rows(), nullptr, std::move(chunk_ctx));
                }

                /// Piggy-back watermark in header's chunk info if there is
                output_chunks.emplace_back(
                    block.getColumns(), block_rows, output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());

                output_header_chunk.setChunkInfo(nullptr);
                output_header_chunk.setChunkContext(nullptr);
            }

            ++i;
        }
    }
}

void JoinTransform::validateAsofJoinKey(const Block & left_input_header, const Block & right_input_header)
{
    auto join_strictness = join->getStreamingStrictness();
    if (join_strictness == Strictness::All || join_strictness == Strictness::Asof || join_strictness == Strictness::Any)
        return;

    const auto & table_join = join->getTableJoin();
    if (table_join.rangeAsofJoinContext().type != RangeType::Interval)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Only time interval range join is supported in stream to stream join. Use `date_diff(...) or date_diff_within(...) function.");

    const auto & join_clause = join->getTableJoin().getOnlyClause();

    auto check_type = [](const Block & header, size_t col_pos) {
        auto type = header.getByPosition(col_pos).type;
        if (!isDateTime64(type) && !isDateTime(type))
            throw DB::Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The range column in stream to stream join only supports datetime64 or datetime column types");
    };

    auto left_col_pos = left_input_header.getPositionByName(join_clause.key_names_left.back());
    auto right_col_pos = right_input_header.getPositionByName(join_clause.key_names_right.back());

    check_type(left_input_header, left_col_pos);
    check_type(right_input_header, right_col_pos);

    const auto & left_col_with_type = left_input_header.getByPosition(left_col_pos);
    const auto & right_col_with_type = right_input_header.getByPosition(right_col_pos);

    if (left_col_with_type.type->getTypeId() != right_col_with_type.type->getTypeId())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The columns in stream to stream range join have different column types");

    UInt32 scale = 0;
    if (isDateTime64(left_col_with_type.type))
    {
        /// Check scale
        auto * left_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(left_col_with_type.type.get());
        assert(left_datetime64_type);

        auto * right_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(right_col_with_type.type.get());
        assert(right_datetime64_type);

        if (left_datetime64_type->getScale() != right_datetime64_type->getScale())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The datetime64 columns in stream to stream range join have different scales");

        scale = left_datetime64_type->getScale();
    }

    join->setAsofJoinColumnPositionAndScale(scale, left_col_pos, right_col_pos, left_col_with_type.type->getTypeId());
}
}
}
