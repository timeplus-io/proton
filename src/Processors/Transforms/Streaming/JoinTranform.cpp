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
    join->transformHeader(header);
    return header;
}

JoinTransform::JoinTransform(
    Block left_input_header,
    Block right_input_header,
    HashJoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_cached_bytes_,
    FinishCounterPtr finish_counter_)
    : IProcessor(
        {left_input_header, right_input_header}, {transformHeader(left_input_header, join_)}, ProcessorID::StreamingJoinTransformID)
    , port_can_have_more_data{true, true}
    , output_header(outputs.front().getHeader())
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , join(std::move(join_))
    , finish_counter(std::move(finish_counter_))
    , max_block_size(max_block_size_)
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

    if (join->rangeBidirectionalHashJoin())
    {
        rangeJoinBidirectionally(std::move(blocks));
    }
    else if (join->bidirectionalHashJoin())
    {
        joinBidirectionally(std::move(blocks));
    }
    else
    {
        /// First insert right block to update the build-side hash table
        if (blocks[1])
            join->insertRightBlock(std::move(blocks[1]));

        /// Then use left block to join the right updated hash table
        /// Please note in this mode, right stream data only changes won't trigger join since left stream data is not buffered
        if (blocks[0])
        {
            join->joinLeftBlock(blocks[0]);
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
}

void JoinTransform::joinBidirectionally(std::vector<Block> && blocks)
{
    /// We need buffer left and right stream data
    /// And we also join each block every time

    auto & left_block = blocks[0];
    if (left_block.rows())
    {
        auto retracted_block = join->insertLeftBlockAndJoin(left_block);
        auto left_block_rows = left_block.rows();
        if (left_block_rows)
        {
            std::scoped_lock lock(mutex);

            /// First emit retracted block
            auto retracted_block_rows = retracted_block.rows();
            if (retracted_block_rows)
            {
                /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
                auto chunk_ctx = std::make_shared<ChunkContext>();
                chunk_ctx->setAvoidWatermark();
                output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
            }

            /// Piggy-back watermark in header's chunk info if there is
            /// We only do this piggy-back once
            output_chunks.emplace_back(
                left_block.getColumns(), left_block_rows, output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());

            output_header_chunk.setChunkInfo(nullptr);
            output_header_chunk.setChunkContext(nullptr);
        }
    }

    auto & right_block = blocks[1];
    if (right_block.rows())
    {
        auto retracted_block = join->insertRightBlockAndJoin(right_block, output_header);
        auto right_block_rows = right_block.rows();
        if (right_block_rows)
        {
            std::scoped_lock lock(mutex);

            /// First emit retracted block
            auto retracted_block_rows = retracted_block.rows();
            if (retracted_block_rows)
            {
                /// Don't watermark this block. We can concat retracted / result blocks or use avoid watermarking
                auto chunk_ctx = std::make_shared<ChunkContext>();
                chunk_ctx->setAvoidWatermark();
                output_chunks.emplace_back(retracted_block.getColumns(), retracted_block_rows, nullptr, std::move(chunk_ctx));
            }

            /// Piggy-back watermark in header's chunk info if there is
            /// We only do this piggy-back once
            output_chunks.emplace_back(
                right_block.getColumns(), right_block_rows, output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());

            output_header_chunk.setChunkInfo(nullptr);
            output_header_chunk.setChunkContext(nullptr);
        }
    }
}

void JoinTransform::rangeJoinBidirectionally(std::vector<Block> && blocks)
{
    auto & left_block = blocks[0];
    if (left_block.rows())
    {
        auto joined_blocks = join->insertLeftBlockToRangeBucketsAndJoin(std::move(left_block));

        std::scoped_lock lock(mutex);

        for (size_t i = 0, size = joined_blocks.size(); i < size; ++i)
        {
            if (i != size - 1)
            {
                output_chunks.emplace_back(joined_blocks[i].getColumns(), joined_blocks[i].rows());
            }
            else
            {
                /// Piggy-back watermark in header's chunk info if there is
                /// We only do this piggy-back once
                output_chunks.emplace_back(
                    joined_blocks[i].getColumns(), joined_blocks[i].rows(), output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());

                output_header_chunk.setChunkInfo(nullptr);
                output_header_chunk.setChunkContext(nullptr);
            }
        }
    }

    auto & right_block = blocks[1];
    if (right_block.rows())
    {
        auto joined_blocks = join->insertRightBlockToRangeBucketsAndJoin(std::move(right_block), output_header);

        std::scoped_lock lock(mutex);

        for (size_t i = 0, size = joined_blocks.size(); i < size; ++i)
        {
            if (i != size - 1)
            {
                output_chunks.emplace_back(joined_blocks[i].getColumns(), joined_blocks[i].rows());
            }
            else
            {
                /// Piggy-back watermark in header's chunk info if there is
                /// We only do this piggy-back once
                output_chunks.emplace_back(
                    joined_blocks[i].getColumns(), joined_blocks[i].rows(), output_header_chunk.getChunkInfo(), output_header_chunk.getChunkContext());

                output_header_chunk.setChunkInfo(nullptr);
                output_header_chunk.setChunkContext(nullptr);
            }
        }
    }
}

void JoinTransform::validateAsofJoinKey(const Block & left_input_header, const Block & right_input_header)
{
    auto join_strictness = join->getStreamingStrictness();
    if (join_strictness == Strictness::All || join_strictness == Strictness::Asof || join_strictness == Strictness::Latest)
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
