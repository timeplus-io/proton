#include <Processors/Transforms/Streaming/JoinTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/HashJoin/ConcurrentHashJoin.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>

namespace DB
{
namespace ErrorCodes
{
extern int CREATE_CHECKPOINT_FAILED;
extern int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
void JoinTransform::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    const auto & settings = ckpt_ctx->request_ctx->settings;

    CheckpointType ckpt_type = CheckpointType::File;
    if (join->type() == HashJoinType::Hybrid && settings->type != CheckpointType::File)
        ckpt_type = CheckpointType::Rocks;
    else if (join->type() == HashJoinType::Memory && settings->type == CheckpointType::Rocks)
        throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Memory hash join does not support checkpoint type 'rocks'");

    CheckpointPtr ckpt;
    if (ckpt_type == CheckpointType::Rocks)
    {
        RocksDBPtr rocks;

        /// Serializing join algorithm state
        if (auto concurrent_join = std::dynamic_pointer_cast<ConcurrentHashJoin>(join))
        {
            concurrent_join->write(getVersion());
            auto * hybrid_hash_join = static_cast<HybridHashJoin *>(concurrent_join->getInternalHashJoin(transform_id)->data.get());
            rocks = hybrid_hash_join->getOrCreateRocksDB();
        }
        else if (auto hybrid_join = std::dynamic_pointer_cast<HybridHashJoin>(join))
        {
            hybrid_join->write(getVersion());
            rocks = hybrid_join->getOrCreateRocksDB();
        }
        else
        {
            throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Unknown join type {}", typeid(join).name());
        }

        /// Reuse the default rocks handler to avoid creating a new one (must gurantee don't overwrite the same key)
        rocks->getDefaultColumnFamilyHandler()->put("_watermark", watermark);

        ckpt = std::make_shared<RocksCheckpoint>(getVersion(), rocks);
    }
    else
    {
        ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
            join->serialize(wb, getVersion());
            DB::writeIntBinary(watermark, wb);
        });
    }

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void JoinTransform::doRecover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::Rocks:
        {
            auto rocks_ckpt = std::static_pointer_cast<RocksCheckpoint>(ckpt);

            HybridHashJoin * hybrid_join;
            if (auto concurrent_join = std::dynamic_pointer_cast<ConcurrentHashJoin>(join))
            {
                auto internal_join = concurrent_join->getInternalHashJoin(transform_id);
                hybrid_join = dynamic_cast<HybridHashJoin *>(internal_join->data.get());
            }
            else
            {
                hybrid_join = dynamic_cast<HybridHashJoin *>(join.get());
            }

            if (!hybrid_join)
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Failed to recover rocks checkpoint since it is not a hybrid hash join");

            /// NOTE: All rocks handlers are invalid after shutdown
            hybrid_join->shutdownRocks();
            rocks_ckpt->recover(hybrid_join->getRocksDir());

            /// Reinstall recovered rocks
            hybrid_join->reinstallRocksDB();
            hybrid_join->read(rocks_ckpt->getVersion());
            hybrid_join->getOrCreateRocksDB()->getDefaultColumnFamilyHandler()->get("_watermark", watermark);
            break;
        }
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) {
                join->deserialize(rb, version_);
                DB::readIntBinary(watermark, rb);
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
}
