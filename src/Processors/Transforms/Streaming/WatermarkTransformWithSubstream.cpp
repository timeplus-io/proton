#include <Processors/Transforms/Streaming/WatermarkTransformWithSubstream.h>

#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>
#include <Interpreters/Streaming/Substream/MemorySubstreamHashMap.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

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
    const Block & header,
    EmitParamsPtr params_,
    bool skip_stamping_for_backfill_data_,
    HashTableType hash_table_type,
    const String & spill_dir,
    size_t max_hot_key_count)
    : IProcessor({header}, {header}, ProcessorID::WatermarkTransformWithSubstreamID)
    , params(std::move(params_))
    , skip_stamping_for_backfill_data(skip_stamping_for_backfill_data_)
    , logger(getLogger("WatermarkTransformWithSubstream"))
    , last_log_ts(MonotonicMilliseconds::now())
{
    chassert(params->mode != EmitMode::None);
    watermark_template = std::make_unique<WatermarkStamper>(*params, logger);
    assert(watermark_template);
    watermark_template->preProcess(header);

    initSubstreamHashMap(hash_table_type, spill_dir, max_hot_key_count);
}

void WatermarkTransformWithSubstream::initSubstreamHashMap(
    HashTableType hash_table_type, const String & spill_dir, size_t max_hot_key_count)
{
    auto value_serializer = [](const void * value, WriteBuffer & wb) {
        const auto & watermark = *reinterpret_cast<const ValueType *>(value);
        watermark->serialize(wb);
        return ErrorCodes::OK;
    };

    auto value_deserializer = [this](void * value, ReadBuffer & rb) {
        auto & watermark = *reinterpret_cast<ValueType *>(value);
        watermark = watermark_template->clone();
        watermark->deserialize(rb);
        return ErrorCodes::OK;
    };

    switch (hash_table_type)
    {
        case HashTableType::Memory:
        {
            substream_watermarks
                = std::make_unique<MemorySubstreamHashMap<ValueType>>(std::move(value_serializer), std::move(value_deserializer));
            return;
        }
        case HashTableType::Hybrid:
        {
            substream_watermarks = std::make_unique<HybridSubstreamHashMap<ValueType>>(
                spill_dir + "_substream", max_hot_key_count, std::move(value_serializer), std::move(value_deserializer), logger);
            return;
        }
    }
    UNREACHABLE();
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

    SCOPE_EXIT({ output_iter = output_chunks.begin(); });

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
        output_chunks.reserve(substream_watermarks->size() + 1);
        /// Propagate historical data end flag first
        output_chunks.emplace_back(process_chunk.clone());

        substream_watermarks->forEachKeyValue(
            [&](const auto & id, auto value) {
                auto chunk_ctx = ChunkContext::create();
                chunk_ctx->setSubstreamID(id);
                process_chunk.setChunkContext(std::move(chunk_ctx)); /// reset context

                auto & watermark = *reinterpret_cast<ValueType *>(value.getMutableMapped());
                watermark->processAfterUnmuted(process_chunk);
                output_chunks.emplace_back(process_chunk.clone());
            },
            /*inmemory=*/false);
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
            std::vector<SubstreamID> expried_substreams;
            substream_watermarks->forEachKeyValue(
                [&](const auto & id, auto value) {
                    auto & watermark = *reinterpret_cast<ValueType *>(value.getMutableMapped());
                    process_chunk.setChunkContext(nullptr); /// clear context, act as a heart beat
                    watermark->process(process_chunk);

                    if (process_chunk.hasChunkContext())
                    {
                        if (process_chunk.getWatermark() == TIMEOUT_WATERMARK)
                            expried_substreams.emplace_back(id);
                        else
                            /// If exists periodic watermark, shall be propagated all substreams
                            output_chunks.reserve(substream_watermarks->size());

                        process_chunk.setSubstreamID(id);
                        output_chunks.emplace_back(process_chunk.clone());
                        propagated_heartbeat = true;
                    }
                },
                /*inmemory=*/false);

            /// Remove expired substreams
            for (auto & id : expried_substreams)
                removeSubstreamWatermark(id);
        }

        if (!propagated_heartbeat)
        {
            process_chunk.setChunkContext(nullptr); /// clear context, act as a heart beat
            output_chunks.emplace_back(std::move(process_chunk));
        }
    }

    if (auto now = MonotonicMilliseconds::now(); now - last_log_ts > log_metrics_interval_ms)
    {
        LOG_INFO(logger, "Substream metrics: {}", substream_watermarks->metricsString());
        last_log_ts = now;
    }
}

WatermarkStamper & WatermarkTransformWithSubstream::getOrCreateSubstreamWatermark(const SubstreamID & id)
{
    auto [value, inserted] = substream_watermarks->emplaceKey(id);
    auto & watermark = *reinterpret_cast<ValueType *>(value);
    if (inserted)
        watermark = watermark_template->clone();

    return *watermark;
}

bool WatermarkTransformWithSubstream::removeSubstreamWatermark(const SubstreamID & id)
{
    return substream_watermarks->removeKey(id);
}

String WatermarkTransformWithSubstream::getName() const
{
    switch (substream_watermarks->type())
    {
        case HashTableType::Memory:
            return fmt::format("{}TransformWithSubstream", watermark_template->getName());
        case HashTableType::Hybrid:
            return fmt::format("Hybrid{}TransformWithSubstream", watermark_template->getName());
    }
}
}
}
