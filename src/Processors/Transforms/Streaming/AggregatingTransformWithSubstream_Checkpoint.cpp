#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
void AggregatingTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    const auto & settings = ckpt_ctx->request_ctx->settings;

    CheckpointType ckpt_type = CheckpointType::File;
    if (substream_aggregates_data->type() == HashTableType::Hybrid && settings->type != CheckpointType::File)
        ckpt_type = CheckpointType::Rocks;
    else if (substream_aggregates_data->type() == HashTableType::Memory && settings->type == CheckpointType::Rocks)
        throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Memory aggregation with substream does not support checkpoint type 'rocks'");

    CheckpointPtr ckpt;
    if (ckpt_type == CheckpointType::Rocks)
        ckpt = assert_cast<Streaming::HybridSubstreamHashMap<ValueType> &>(*substream_aggregates_data).createRocksCheckpoint(getVersion());
    else
        ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) { substream_aggregates_data->serialize(wb); });

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void AggregatingTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::Rocks:
        {
            if (substream_aggregates_data->type() != HashTableType::Hybrid)
                throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Rocks checkpoint only support with hybrid hash table");

            assert_cast<Streaming::HybridSubstreamHashMap<ValueType> &>(*substream_aggregates_data)
                .recoverFromRocksCheckpoint(std::static_pointer_cast<RocksCheckpoint>(ckpt));
            break;
        }
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) { substream_aggregates_data->deserialize(rb); });
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
