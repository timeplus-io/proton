#include "StreamingJoinTranform.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/join_common.h>

/// #include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_BYTES;
}

Block StreamingJoinTransform::transformHeader(Block header, const StreamingHashJoinPtr & join)
{
    /// LOG_DEBUG(&Poco::Logger::get("StreamingJoinTransform"), "Before join block: '{}'", header.dumpStructure());
    join->checkTypesOfKeys(header);
    ExtraBlockPtr tmp;
    join->joinBlock(header, tmp);
    /// LOG_DEBUG(&Poco::Logger::get("StreamingJoinTransform"), "After join block: '{}'", header.dumpStructure());
    return header;
}

StreamingJoinTransform::StreamingJoinTransform(
    Block left_input_header,
    Block right_input_header,
    StreamingHashJoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_wait_ms_,
    UInt64 join_max_wait_rows_,
    UInt64 join_max_cached_bytes_,
    FinishCounterPtr finish_counter_)
    : IProcessor({left_input_header, right_input_header}, {transformHeader(left_input_header, join_)})
    , insert_funcs({&StreamingHashJoin::insertLeftBlock, &StreamingHashJoin::insertRightBlock})
    , port_can_have_more_data{true, true}
    , header_chunk(outputs.front().getHeader().getColumns(), 0)
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

IProcessor::Status StreamingJoinTransform::prepare()
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

void StreamingJoinTransform::work()
{
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
                }

                if (port_ctx.input_chunk.hasWatermark())
                    /// We will use one of the watermark if both of them are present at port
                    /// FIXME
                    header_chunk.setChunkInfo(port_ctx.input_chunk.getChunkInfo());

                port_ctx.has_input = false;
            }

            ++i;
        }
    }

    if (!join->needBufferLeftStream())
    {
        /// First insert right block to update the build-side hash table
        if (blocks[1])
            join->insertRightBlock(std::move(blocks[1]));

        /// Then use left block to join the right updated hash table
        if (blocks[0])
        {
            ExtraBlockPtr extra_block;
            join->joinBlock(blocks[0], extra_block);
            if (blocks[0])
            {
                std::scoped_lock lock(mutex);
                /// Piggy-back watermark in header's chunk info if there is
                output_chunks.emplace_back(blocks[0].getColumns(), blocks[0].rows(), header_chunk.getChunkInfo());
                header_chunk.setChunkInfo(nullptr);
            }
        }
    }
    else
    {
        bufferDataAndJoin(std::move(blocks));
    }
}

void StreamingJoinTransform::bufferDataAndJoin(std::vector<Block> && blocks)
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
            output_chunks.emplace_back(block.getColumns(), block.rows(), header_chunk.getChunkInfo());
            header_chunk.setChunkInfo(nullptr);
        }

        /// Join may trigger data recycling
        /// If cached bytes drop below threshold, clear the no more data flag
        port_can_have_more_data[0] = left_cached_bytes <= join_max_cached_bytes;
        port_can_have_more_data[1] = right_cached_bytes <= join_max_cached_bytes;
    }

    {
        std::scoped_lock lock(mutex);
        if (only_timer_blocks || header_chunk.hasWatermark())
        {
            /// We like to propagate the empty timer block as well
            /// FIXME, when high performance timer is ready, we probably don't need pass along the empty timer block
            /// in the pipeline
            output_chunks.emplace_back(header_chunk.clone());
            header_chunk.setChunkInfo(nullptr);
        }
    }

    if (!port_can_have_more_data[0] && !port_can_have_more_data[1])
        /// Both stream are full of data, to avoid deadlock, throw
        throw Exception(
            ErrorCodes::TOO_MANY_BYTES, "There are too much data cached for both left stream and right stream in the stream join");
}

bool StreamingJoinTransform::timeToJoin() const
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

void StreamingJoinTransform::validateAsofJoinKey(const Block & left_input_header, const Block & right_input_header)
{
    auto join_strictness = join->getStrictness();
    if (join_strictness == ASTTableJoin::Strictness::All || join_strictness == ASTTableJoin::Strictness::StreamingAsof
        || join_strictness == ASTTableJoin::Strictness::StreamingAny)
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
