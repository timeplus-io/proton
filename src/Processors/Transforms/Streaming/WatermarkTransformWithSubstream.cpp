#include "WatermarkTransformWithSubstream.h"
#include "HopWatermark.h"
#include "SessionWatermark.h"
#include "TumbleWatermark.h"

#include <Columns/ColumnDecimal.h>
#include <Common/ProtonCommon.h>

/// FIXME: Week / Month / Quarter / Year cases don't work yet
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
    const std::vector<size_t> & substream_keys,
    const Block & input_header,
    const Block & output_header,
    Poco::Logger * log_)
    : IProcessor({input_header}, {std::move(output_header)})
    , log(log_)
    , header(input_header)
    , substream_splitter(std::move(input_header), substream_keys)
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

std::pair<Int64, Int64> WatermarkTransformWithSubstream::calcMinMaxEventTime(const Block & block) const
{
    if (time_col_is_datetime64)
    {
        const auto & time_col = block.getByPosition(time_col_pos);
        auto col = assert_cast<ColumnDecimal<DateTime64> *>(time_col.column->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {min_max_ts.first->value, min_max_ts.second->value};
    }
    else
    {
        const auto & time_col = block.getByPosition(time_col_pos);
        auto col = assert_cast<ColumnVector<UInt32> *>(time_col.column->assumeMutable().get());
        auto min_max_ts{std::minmax_element(col->getData().begin(), col->getData().end())};
        return {*min_max_ts.first, *min_max_ts.second};
    }
}

void WatermarkTransformWithSubstream::work()
{
    auto block = header.cloneWithColumns(input_chunk.detachColumns());
    auto splitted_blocks{substream_splitter(block)};

    assert(output_iter == output_chunks.end());
    output_chunks.clear();
    output_chunks.reserve(splitted_blocks.size());
    for (auto & [id, sub_block] : splitted_blocks)
    {
        auto & watermark = getOrCreateSubstreamWatermark(id);
        watermark.process(sub_block);

        /// Keep substream id for per sub-block, used for downstream processors
        auto chunk_info = std::make_shared<ChunkInfo>();
        chunk_info->ctx.setWatermark(WatermarkBound{id, sub_block.info.watermark, sub_block.info.watermark_lower_bound});
        Chunk chunk(sub_block.getColumns(), sub_block.rows(), std::move(chunk_info));
        output_chunks.emplace_back(std::move(chunk));
    }

    /// Only for session watermark, the SessionAggregatingTransform need current event time bound to check oversize session
    /// So we add an empty chunk with min/max event time
    if (emit_min_max_event_time && block.rows() > 0)
    {
        auto min_max_ts = calcMinMaxEventTime(block);
        auto chunk_info = std::make_shared<ChunkInfo>();
        chunk_info->ctx.setWatermark(WatermarkBound{INVALID_SUBSTREAM_ID, min_max_ts.second, min_max_ts.first});
        Chunk chunk(getOutputs().front().getHeader().getColumns(), 0, std::move(chunk_info));
        output_chunks.emplace_back(std::move(chunk));
    }

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

        watermark_template = std::make_unique<SessionWatermark>(std::move(watermark_settings), proc_time, desc->session_start, desc->session_end, log);
        watermark_name = "SessionWatermark";
    }
    else
    {
        watermark_template = std::make_unique<Watermark>(std::move(watermark_settings), proc_time, log);
        watermark_name = "Watermark";
    }
}
}
}
