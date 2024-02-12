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
WatermarkStamperPtr initWatermark(const WatermarkStamperParams & params, Poco::Logger * logger)
{
    assert(params.mode != EmitMode::None);
    if (params.window_params)
    {
        switch (params.window_params->type)
        {
            case WindowType::Tumble:
                return std::make_unique<TumbleWatermarkStamper>(params, logger);
            case WindowType::Hop:
                return std::make_unique<HopWatermarkStamper>(params, logger);
            case WindowType::Session:
                return std::make_unique<SessionWatermarkStamper>(params, logger);
            default:
                break;
        }
    }
    return std::make_unique<WatermarkStamper>(params, logger);
}
}

WatermarkTransformWithSubstream::WatermarkTransformWithSubstream(
    const Block & header, WatermarkStamperParamsPtr params_, bool skip_stamping_for_backfill_data_, Poco::Logger * logger)
    : IProcessor({header}, {header}, ProcessorID::WatermarkTransformWithSubstreamID)
    , params(std::move(params_))
    , skip_stamping_for_backfill_data(skip_stamping_for_backfill_data_)
{
    watermark_template = initWatermark(*params, logger);
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
        {
            output.finish();
            return Status::Finished;
        }

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

    process_chunk.clearWatermark();

    if (process_chunk.isHistoricalDataStart() && skip_stamping_for_backfill_data) [[unlikely]]
    {
        mute_watermark = true;
        /// Propagate historical data start flag
        output_chunks.emplace_back(std::move(process_chunk));
        return;
    }

    if (process_chunk.isHistoricalDataEnd() && skip_stamping_for_backfill_data) [[unlikely]]
    {
        mute_watermark = false;
        output_chunks.reserve(substream_watermarks.size() + 1);
        /// Propagate historical data end flag first
        output_chunks.emplace_back(process_chunk.clone());
        for (auto & [id, watermark] : substream_watermarks)
        {
            auto chunk_ctx = ChunkContext::create();
            chunk_ctx->setSubstreamID(std::move(id));
            process_chunk.setChunkContext(std::move(chunk_ctx)); /// reset context

            watermark->processAfterUnmuted(process_chunk);
            output_chunks.emplace_back(process_chunk.clone());
        }
        return;
    }

    if (unlikely(process_chunk.requestCheckpoint()))
    {
        checkpoint(process_chunk.getCheckpointContext());
        output_chunks.emplace_back(std::move(process_chunk));
    }
    else if (process_chunk.hasRows())
    {
        assert(process_chunk.hasChunkContext());

        auto & watermark = getOrCreateSubstreamWatermark(process_chunk.getSubstreamID());

        if (!process_chunk.avoidWatermark())
        {
            if (mute_watermark)
                watermark.processWithMutedWatermark(process_chunk);
            else
                watermark.process(process_chunk);
        }

        assert(process_chunk);
        output_chunks.emplace_back(std::move(process_chunk));
    }
    else
    {
        /// FIXME, we shall establish timer only when necessary instead of blindly generating empty heartbeat chunk
        bool propagated_heartbeat = false;

        /// It's possible to generate periodic or timeout watermark for each substream via an empty chunk
        /// FIXME: This is a very ugly and inefficient implementation and needs to revisit.
        if (!mute_watermark && watermark_template->requiresPeriodicOrTimeoutEmit())
        {
            output_chunks.reserve(substream_watermarks.size());
            for (auto & [id, watermark] : substream_watermarks)
            {
                process_chunk.setChunkContext(nullptr); /// clear context, act as a heart beat
                watermark->process(process_chunk);

                if (process_chunk.hasChunkContext())
                {
                    process_chunk.trySetSubstreamID(id);
                    output_chunks.emplace_back(process_chunk.clone());
                    propagated_heartbeat = true;
                }
            }
        }

        if (!propagated_heartbeat)
        {
            process_chunk.setChunkContext(nullptr); /// clear context, act as a heart beat
            output_chunks.emplace_back(std::move(process_chunk));
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
