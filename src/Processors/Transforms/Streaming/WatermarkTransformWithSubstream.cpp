#include "WatermarkTransformWithSubstream.h"
#include "HopWatermark.h"
#include "SessionWatermark.h"
#include "TumbleWatermark.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ProtonCommon.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_EMIT_MODE;
extern const int UNSUPPORTED;
}

namespace Streaming
{
WatermarkTransformWithSubstream::WatermarkTransformWithSubstream(
    ASTPtr query,
    TreeRewriterResultPtr syntax_analyzer_result,
    FunctionDescriptionPtr desc,
    bool proc_time,
    std::vector<size_t> key_column_positions,
    const Block & input_header,
    const Block & output_header,
    Poco::Logger * log_)
    : IProcessor({input_header}, {std::move(output_header)}, ProcessorID::WatermarkTransformWithSubstreamID)
    , header(input_header)
    , substream_splitter(std::move(key_column_positions))
    , log(log_)
{
    initWatermark(query, syntax_analyzer_result, desc, proc_time);

    assert(watermark_template);
    watermark_template->preProcess();

    if (watermark_name == "SessionWatermark")
    {
        emit_min_max_event_time = true;
        time_col_is_datetime64 = isDateTime64(desc->argument_types[0]);
        time_col_pos = input_header.getPositionByName(desc->argument_names[0]);
    }
}

IProcessor::Status WatermarkTransformWithSubstream::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

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

    /// Has substream chunk(s) that needs to output
    if (output_iter != output_chunks.end())
    {
        output.push(std::move(*(output_iter++)));
        return Status::PortFull;
    }

    /// Check can input.
    if (!input_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        input_chunk = input.pull(true);
    }

    /// Now consume.
    return Status::Ready;
}

std::pair<Int64, Int64> WatermarkTransformWithSubstream::calcMinMaxEventTime(const Chunk & chunk) const
{
    if (time_col_is_datetime64)
    {
        const auto & time_col = chunk.getColumns()[time_col_pos];
        auto col = assert_cast<ColumnDecimal<DateTime64> *>(time_col->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {min_max_ts.first->value, min_max_ts.second->value};
    }
    else
    {
        const auto & time_col = chunk.getColumns()[time_col_pos];
        auto col = assert_cast<ColumnVector<UInt32> *>(time_col->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {*min_max_ts.first, *min_max_ts.second};
    }
}

void WatermarkTransformWithSubstream::work()
{
    /// Only for session watermark, the SessionAggregatingTransform need current event time bound to check oversize session
    /// So we add an empty chunk with min/max event time
    /// We will need clear input_chunk for next run
    Chunk process_chunk;
    process_chunk.swap(input_chunk);

    Chunk min_max_chunk;
    if (emit_min_max_event_time && process_chunk.hasRows())
    {
        auto min_max_ts = calcMinMaxEventTime(process_chunk);

        auto chunk_ctx = std::make_shared<ChunkContext>();
        chunk_ctx->setWatermark(WatermarkBound{INVALID_SUBSTREAM_ID, min_max_ts.second, min_max_ts.first});

        Chunk chunk(getOutputs().front().getHeader().getColumns(), 0);
        chunk.setChunkContext(std::move(chunk_ctx));
        min_max_chunk.swap(chunk);
    }

    auto split_chunks{substream_splitter.split(process_chunk)};

    assert(output_iter == output_chunks.end());
    output_chunks.clear();
    output_chunks.reserve(split_chunks.size());

    for (auto & chunk_with_id : split_chunks)
    {
        auto & watermark = getOrCreateSubstreamWatermark(chunk_with_id.id);

        /// FIXME, watermark shall accept a chunk instead of a block
        Block sub_block = header.cloneWithColumns(chunk_with_id.chunk.detachColumns());
        watermark.process(sub_block);

        /// Keep substream id for per sub-block, used for downstream processors
        Chunk sub_chunk(sub_block.getColumns(), sub_block.rows());
        auto chunk_ctx = std::make_shared<ChunkContext>();
        chunk_ctx->setWatermark(
            WatermarkBound{std::move(chunk_with_id.id), sub_block.info.watermark, sub_block.info.watermark_lower_bound});
        sub_chunk.setChunkContext(std::move(chunk_ctx));
        output_chunks.emplace_back(std::move(sub_chunk));
    }

    if (min_max_chunk)
        output_chunks.emplace_back(std::move(min_max_chunk));

    output_iter = output_chunks.begin(); /// need to output chunks
}

Watermark & WatermarkTransformWithSubstream::getOrCreateSubstreamWatermark(const SubstreamID & id)
{
    auto iter = substream_watermarks.find(id);
    if (iter == substream_watermarks.end())
        return *(substream_watermarks.emplace(id, watermark_template->clone()).first->second);

    return *(iter->second);
}

void WatermarkTransformWithSubstream::initWatermark(
    ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time)
{
    WatermarkSettings watermark_settings(query, syntax_analyzer_result, desc);
    if (watermark_settings.func_name == ProtonConsts::TUMBLE_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark_template = std::make_unique<TumbleWatermark>(std::move(watermark_settings), proc_time, log);
        watermark_name = "TumbleWatermark";
    }
    else if (watermark_settings.func_name == ProtonConsts::HOP_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark_template = std::make_unique<HopWatermark>(std::move(watermark_settings), proc_time, log);
        watermark_name = "HopWatermark";
    }
    else if (watermark_settings.func_name == ProtonConsts::SESSION_FUNC_NAME)
    {
        if (watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK
            && watermark_settings.mode != WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY)
            throw Exception("Streaming window functions only support watermark based emit", ErrorCodes::INVALID_EMIT_MODE);

        watermark_template
            = std::make_unique<SessionWatermark>(std::move(watermark_settings), proc_time, desc->session_start, desc->session_end, log);
        watermark_name = "SessionWatermark";
    }
    else
    {
        watermark_template = std::make_unique<Watermark>(std::move(watermark_settings), proc_time, log);
        watermark_name = "Watermark";
    }
}

void WatermarkTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), logic_pid, ckpt_ctx, [this](WriteBuffer & wb) {
        writeIntBinary(substream_watermarks.size(), wb);

        for (const auto & [id, watermark] : substream_watermarks)
        {
            serialize(id, wb);
            watermark->serialize(wb);
        }
    });
}

void WatermarkTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(logic_pid, ckpt_ctx, [this](VersionType, ReadBuffer & rb) {
        size_t size = 0;
        readIntBinary(size, rb);

        substream_watermarks.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            SubstreamID substream_id{deserialize(rb)};

            auto watermark = watermark_template->clone();
            watermark->deserialize(rb);
            substream_watermarks.emplace(std::move(substream_id), std::move(watermark));
        }
    });
}
}
}
