#include "WatermarkTransformWithSubstream.h"
#include "HopWatermark.h"
#include "TumbleWatermark.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
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
    const Block & input_header,
    const Block & output_header,
    Poco::Logger * log_)
    : IProcessor({input_header}, {output_header}, ProcessorID::WatermarkTransformWithSubstreamID)
    , header(input_header)
    , log(log_)
{
    initWatermark(input_header, query, syntax_analyzer_result, desc, proc_time);

    assert(watermark_template);
    watermark_template->preProcess();
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
        output.push(std::move(*output_iter));
        ++output_iter;
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

void WatermarkTransformWithSubstream::work()
{
    assert(output_iter == output_chunks.end());
    output_chunks.clear();

    /// We will need clear input_chunk for next run
    Chunk process_chunk;
    process_chunk.swap(input_chunk);
    if (process_chunk.hasRows())
    {
        assert(process_chunk.hasChunkContext());

        auto & watermark = getOrCreateSubstreamWatermark(process_chunk.getSubstreamID());

        watermark.process(process_chunk);
        assert(process_chunk);

        output_chunks.emplace_back(std::move(process_chunk));
    }
    else
    {
        /// FIXME, we shall establish timer only when necessary instead of blindly generating empty heartbeat chunk
        output_chunks.reserve(substream_watermarks.size());
        for (auto & [id, watermark] : substream_watermarks)
        {
            auto chunk = process_chunk.clone();
            watermark->process(chunk);

            if (chunk.hasWatermark())
            {
                chunk.getChunkContext()->setSubstreamID(id);
                output_chunks.emplace_back(std::move(chunk));
            }
        }
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
    const Block & input_header, ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, FunctionDescriptionPtr desc, bool proc_time)
{
    WatermarkSettings watermark_settings(query, syntax_analyzer_result, desc);
    if (watermark_settings.isTumbleWindowAggr())
    {
        assert(watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK || watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY);
        auto time_col_position = input_header.getPositionByName(desc->argument_names[0]);
        watermark_template = std::make_unique<TumbleWatermark>(std::move(watermark_settings), time_col_position, proc_time, log);
        watermark_name = "TumbleWatermark";
    }
    else if (watermark_settings.isHopWindowAggr())
    {
        assert(watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK || watermark_settings.mode == WatermarkSettings::EmitMode::WATERMARK_WITH_DELAY);
        auto time_col_position = input_header.getPositionByName(desc->argument_names[0]);
        watermark_template = std::make_unique<HopWatermark>(std::move(watermark_settings), time_col_position, proc_time, log);
        watermark_name = "HopWatermark";
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
