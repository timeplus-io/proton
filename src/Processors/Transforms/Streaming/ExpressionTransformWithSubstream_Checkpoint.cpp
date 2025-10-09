#include <Processors/Transforms/Streaming/ExpressionTransformWithSubstream.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/Substream/HybridSubstreamHashMap.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

void ExpressionTransformWithSubstream::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(isStreaming());
    if (!hasState())
        return IProcessor::checkpoint(std::move(ckpt_ctx));

    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    const auto & settings = ckpt_ctx->request_ctx->settings;

    CheckpointType ckpt_type = CheckpointType::File;
    if (substream_stateful_functions->type() == HashTableType::Hybrid && settings->type != CheckpointType::File)
        ckpt_type = CheckpointType::Rocks;
    else if (substream_stateful_functions->type() == HashTableType::Memory && settings->type == CheckpointType::Rocks)
        throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Memory expression with substream does not support checkpoint type 'rocks'");

    CheckpointPtr ckpt;
    if (ckpt_type == CheckpointType::Rocks)
    {
        ckpt = assert_cast<Streaming::HybridSubstreamHashMap<ValueType> &>(*substream_stateful_functions)
                   .createRocksCheckpoint(getVersion());
    }
    else
    {
        ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
            writeVarUInt(stateful_function_builders.size(), wb);
            substream_stateful_functions->serialize(wb);
        });
    }

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void ExpressionTransformWithSubstream::recover(CheckpointContextPtr ckpt_ctx)
{
    if (!isStreaming() || !hasState())
        return;

    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::Rocks:
        {
            if (substream_stateful_functions->type() != HashTableType::Hybrid)
                throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Rocks checkpoint only support with hybrid hash table");

            assert_cast<Streaming::HybridSubstreamHashMap<ValueType> &>(*substream_stateful_functions)
                .recoverFromRocksCheckpoint(std::static_pointer_cast<RocksCheckpoint>(ckpt));
            break;
        }
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) {
                size_t function_size = 0;
                readVarUInt(function_size, rb);
                if (function_size != stateful_function_builders.size())
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Failed to recover expression actions checkpoint. Number of stateful functions are not the same, checkpointed={}, "
                        "current={}",
                        function_size,
                        stateful_function_builders.size());

                substream_stateful_functions->deserialize(rb);
            });
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unknown checkpoint type {}", ckpt->type());
        }
    }
}

}
