#include <Processors/Transforms/Streaming/JoinTranform.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionAnalyzer.h>
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
    Block output_header,
    HashJoinPtr join_,
    size_t max_block_size_,
    UInt64 join_max_cached_bytes)
    : IProcessor({left_input_header, right_input_header}, {output_header}, ProcessorID::StreamingJoinTransformID)
    , join(std::move(join_))
    , max_block_size(max_block_size_)
    , output_header_chunk(outputs.front().getHeader().getColumns(), 0)
    , logger(&Poco::Logger::get("StreamingJoinTransform"))
    , input_ports_with_data{InputPortWithData{&inputs.front()}, InputPortWithData{&inputs.back()}}
{
    assert(join);

    /// Validate asof join column data type
    validateAsofJoinKey(left_input_header, right_input_header);

    /// We know the finalized left header, output header etc, post init HashJoin
    join->postInit(left_input_header, output_header, join_max_cached_bytes);
}

IProcessor::Status JoinTransform::prepare()
{
    std::scoped_lock lock(mutex);

    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        output.finish();

        for (auto & port_ctx : input_ports_with_data)
            port_ctx.input_port->close();

        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & port_ctx : input_ports_with_data)
            port_ctx.input_port->setNotNeeded();

        return Status::PortFull;
    }

    if (!output_chunks.empty())
    {
        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
        return Status::PortFull;
    }

    Status status = Status::NeedData;

    for (size_t i = 0; auto & input_port_with_data : input_ports_with_data)
    {
        if (input_port_with_data.input_chunk)
        {
            return Status::Ready;
        }
        else if (input_port_with_data.input_port->isFinished())
        {
            output.finish();
            /// Close the other input port
            input_ports_with_data[(i + 1) % input_ports_with_data.size()].input_port->close();
            return Status::Finished;
        }
        else
        {
            input_port_with_data.input_port->setNeeded();

            if (input_port_with_data.input_port->hasData())
            {
                /// After pulling the data, we set NotNeeded flag bit to avoid data pull
                /// in-balance between the 2 streams
                input_port_with_data.input_chunk = input_port_with_data.input_port->pull(true);
                status = Status::Ready;
            }
        }
        ++i;
    }

    return status;
}

void JoinTransform::work()
{
    int64_t local_watermark_lower_bound = std::numeric_limits<int64_t>::max();
    int64_t local_watermark_upper_bound = std::numeric_limits<int64_t>::max();

    bool has_watermark = false;
    bool has_data = false;

    Chunks chunks;
    {
        std::scoped_lock lock(mutex);

        assert(input_ports_with_data[0].input_chunk || input_ports_with_data[1].input_chunk);

        for (size_t i = 0; i < input_ports_with_data.size(); ++i)
        {
            if (input_ports_with_data[i].input_chunk)
            {
                if (input_ports_with_data[i].input_chunk.hasWatermark())
                {
                    auto watermark_bound = input_ports_with_data[i].input_chunk.getChunkContext()->getWatermarkWithoutSubstreamID();

                    /// We need lower for both
                    local_watermark_lower_bound = std::min(local_watermark_lower_bound, watermark_bound.second);
                    local_watermark_upper_bound = std::min(local_watermark_upper_bound, watermark_bound.first);
                    has_watermark = true;
                }

                if (input_ports_with_data[i].input_chunk.hasRows())
                    has_data = true;

                chunks[i].swap(input_ports_with_data[i].input_chunk);
            }
        }

        /// We propagate empty chunk with or without watermark
        if (!has_data)
            output_chunks.emplace_back(output_header_chunk.clone());
    }

    assert(local_watermark_lower_bound <= local_watermark_upper_bound);

    if (has_data)
        doJoin(std::move(chunks));

    /// Piggy-back watermark
    /// We only do this piggy-back once for the last output chunk if there is
    if (has_watermark)
    {
        std::scoped_lock lock(mutex);
        if (!output_chunks.empty())
            setupWatermark(output_chunks.back(), local_watermark_lower_bound, local_watermark_upper_bound);
        else
            /// If there is no join result or chunks don't have data but have watermark, we still need propagate the watermark
            propagateWatermark(local_watermark_lower_bound, local_watermark_upper_bound);
    }
}

void JoinTransform::propagateWatermark(int64_t local_watermark_lower_bound, int64_t local_watermark_upper_bound)
{
    auto chunk = output_header_chunk.clone();
    if (setupWatermark(chunk, local_watermark_lower_bound, local_watermark_upper_bound))
        output_chunks.emplace_back(std::move(chunk));
}

inline bool JoinTransform::setupWatermark(Chunk & chunk, int64_t local_watermark_lower_bound, int64_t local_watermark_upper_bound)
{
    /// Watermark shall never regress
    if (local_watermark_upper_bound > watermark_upper_bound && local_watermark_lower_bound > watermark_lower_bound)
    {
        watermark_upper_bound = local_watermark_upper_bound;
        watermark_lower_bound = local_watermark_lower_bound;

        /// Propagate it
        chunk.getOrCreateChunkContext()->setWatermark(watermark_upper_bound, watermark_lower_bound);
        return true;
    }
    return false;
}

inline void JoinTransform::doJoin(Chunks chunks)
{
    if (join->rangeBidirectionalHashJoin())
    {
        rangeJoinBidirectionally(std::move(chunks));
    }
    else if (join->bidirectionalHashJoin())
    {
        joinBidirectionally(std::move(chunks));
    }
    else
    {
        /// First insert right block to update the build-side hash table
        if (chunks[1])
            join->insertRightBlock(input_ports_with_data[1].input_port->getHeader().cloneWithColumns(chunks[1].detachColumns()));

        /// Then use left block to join the right updated hash table
        /// Please note in this mode, right stream data only changes won't trigger join since left stream data is not buffered
        if (chunks[0])
        {
            auto joined_block = input_ports_with_data[0].input_port->getHeader().cloneWithColumns(chunks[0].detachColumns());
            join->joinLeftBlock(joined_block);

            if (auto rows = joined_block.rows(); rows > 0)
            {
                std::scoped_lock lock(mutex);
                output_chunks.emplace_back(joined_block.getColumns(), rows);
            }
        }
    }
}

inline void JoinTransform::joinBidirectionally(Chunks chunks)
{
    std::array<decltype(&Streaming::HashJoin::insertLeftBlockAndJoin), 2> join_funcs
        = {&Streaming::HashJoin::insertLeftBlockAndJoin, &Streaming::HashJoin::insertRightBlockAndJoin};

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        if (chunks[i].getNumRows() == 0)
            continue;

        auto block = input_ports_with_data[i].input_port->getHeader().cloneWithColumns(chunks[i].detachColumns());
        auto retracted_block = std::invoke(join_funcs[i], join.get(), block);

        if (auto rows = block.rows(); rows > 0)
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

            output_chunks.emplace_back(block.getColumns(), rows);
        }
    }
}

inline void JoinTransform::rangeJoinBidirectionally(Chunks chunks)
{
    std::array<decltype(&Streaming::HashJoin::insertLeftBlockToRangeBucketsAndJoin), 2> join_funcs
        = {&Streaming::HashJoin::insertLeftBlockToRangeBucketsAndJoin, &Streaming::HashJoin::insertRightBlockToRangeBucketsAndJoin};

    for (size_t i = 0; i < chunks.size(); ++i)
    {
        if (chunks[i].getNumRows() == 0)
            continue;

        auto block = input_ports_with_data[i].input_port->getHeader().cloneWithColumns(chunks[i].detachColumns());
        auto joined_blocks = std::invoke(join_funcs[i], join.get(), block);

        std::scoped_lock lock(mutex);

        for (size_t j = 0; j < joined_blocks.size(); ++j)
            output_chunks.emplace_back(joined_blocks[j].getColumns(), joined_blocks[j].rows());
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
