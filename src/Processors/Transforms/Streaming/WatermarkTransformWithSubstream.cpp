#include <Processors/Transforms/Streaming/WatermarkTransformWithSubstream.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Transforms/Streaming/HopWatermarkStamper.h>
#include <Processors/Transforms/Streaming/SessionWatermarkStamper.h>
#include <Processors/Transforms/Streaming/TumbleWatermarkStamper.h>
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
namespace
{
WatermarkStamperPtr initWatermark(WatermarkStamperParams params, Poco::Logger * log)
{
    assert(params.mode != WatermarkStamperParams::EmitMode::NONE);
    if (params.window_params)
    {
        switch (params.window_params->type)
        {
            case WindowType::TUMBLE:
                return std::make_unique<TumbleWatermarkStamper>(std::move(params), log);
            case WindowType::HOP:
                return std::make_unique<HopWatermarkStamper>(std::move(params), log);
            case WindowType::SESSION:
                return std::make_unique<SessionWatermarkStamper>(std::move(params), log);
            default:
                break;
        }
    }
    return std::make_unique<WatermarkStamper>(std::move(params), log);
}
}

WatermarkTransformWithSubstream::WatermarkTransformWithSubstream(const Block & header, WatermarkStamperParams params, Poco::Logger * log_)
    : IProcessor({header}, {header}, ProcessorID::WatermarkTransformWithSubstreamID), log(log_)
{
    watermark_template = initWatermark(std::move(params), log);
    assert(watermark_template);
    watermark_template->preProcess(header);
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

            if (chunk.hasChunkContext())
            {
                chunk.getChunkContext()->setSubstreamID(id);
                output_chunks.emplace_back(std::move(chunk));
            }
        }
    }

    output_iter = output_chunks.begin(); /// need to output chunks
}

WatermarkStamper & WatermarkTransformWithSubstream::getOrCreateSubstreamWatermark(const SubstreamID & id)
{
    auto iter = substream_watermarks.find(id);
    if (iter == substream_watermarks.end())
        return *(substream_watermarks.emplace(id, watermark_template->clone()).first->second);

    return *(iter->second);
}

void WatermarkTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    assert(output_chunks.empty());
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
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
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType, ReadBuffer & rb) {
        size_t size = 0;
        readIntBinary(size, rb);

        substream_watermarks.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            SubstreamID substream_id{};
            deserialize(substream_id, rb);

            auto watermark = watermark_template->clone();
            watermark->deserialize(rb);
            substream_watermarks.emplace(std::move(substream_id), std::move(watermark));
        }
    });
}
}
}
