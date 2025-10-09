#include <Processors/Transforms/Streaming/WatermarkTransformWithSubstream.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>
#include <Common/ProtonCommon.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
void WatermarkTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    /// We always push output_chunks first, so we can assume no output_chunks when received request checkpoint
    chassert(output_chunks.empty());

    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    const auto & settings = ckpt_ctx->request_ctx->settings;

    CheckpointType ckpt_type = CheckpointType::File;
    if (substream_watermarks->type() == HashTableType::Hybrid && settings->type != CheckpointType::File)
        ckpt_type = CheckpointType::Rocks;
    else if (substream_watermarks->type() == HashTableType::Memory && settings->type == CheckpointType::Rocks)
        throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Memory watermark with substream does not support checkpoint type 'rocks'");

    CheckpointPtr ckpt;
    if (ckpt_type == CheckpointType::Rocks)
        ckpt = assert_cast<HybridSubstreamHashMap<ValueType> &>(*substream_watermarks).createRocksCheckpoint(getVersion());
    else
        ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) { substream_watermarks->serialize(wb); });

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void WatermarkTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::Rocks:
        {
            if (substream_watermarks->type() != HashTableType::Hybrid)
                throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Rocks checkpoint only support with hybrid hash table");

            assert_cast<HybridSubstreamHashMap<ValueType> &>(*substream_watermarks)
                .recoverFromRocksCheckpoint(std::static_pointer_cast<RocksCheckpoint>(ckpt));
            break;
        }
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) { substream_watermarks->deserialize(rb); });
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unknown checkpoint type {}", ckpt->type());
        }
    }
}
}
}
