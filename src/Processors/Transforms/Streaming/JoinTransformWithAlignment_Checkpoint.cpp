#include <Processors/Transforms/Streaming/JoinTransformWithAlignment.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/HashJoin/ConcurrentHashJoin.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>
#include <Common/VersionRevision.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
void JoinTransformWithAlignment::checkpoint(CheckpointContextPtr ckpt_ctx)
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
        RocksPtr rocks;

        /// Serializing join algorithm state
        if (auto concurrent_join = std::dynamic_pointer_cast<ConcurrentHashJoin>(join))
        {
            concurrent_join->write(getVersion());
            auto * hybrid_hash_join = static_cast<HybridHashJoin *>(concurrent_join->getInternalHashJoin(transform_id)->data.get());
            rocks = hybrid_hash_join->getOrCreateRocks();
        }
        else if (auto hybrid_join = std::dynamic_pointer_cast<HybridHashJoin>(join))
        {
            hybrid_join->write(getVersion());
            rocks = hybrid_join->getOrCreateRocks();
        }
        else
        {
            throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Unknown join type {}", typeid(join).name());
        }

        /// Reuse the default rocks handler to avoid creating a new one (must gurantee don't overwrite the same key)
        auto rocks_handler = rocks->getOrCreateHandler();
        /// Serializing left_input state
        {
            WriteBufferFromOwnString wb;
            left_input.serialize(wb);
            rocks_handler->put("_left_input", wb.str());
        }

        /// Serializing right_input state
        {
            WriteBufferFromOwnString wb;
            right_input.serialize(wb);
            rocks_handler->put("_right_input", wb.str());
        }

        ckpt = std::make_shared<RocksCheckpoint>(getVersion(), rocks);
    }
    else
    {
        ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
            /// Serializing join algorithm state
            join->serialize(wb, getVersion());

            /// Serializing left_input state
            left_input.serialize(wb);

            /// Serializing right_input state
            right_input.serialize(wb);
        });
    }

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void JoinTransformWithAlignment::recover(CheckpointContextPtr ckpt_ctx)
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
            hybrid_join->reinstallRocks();

            /// Deserializing join algorithm state
            hybrid_join->read(rocks_ckpt->getVersion());

            auto rocks_handler = hybrid_join->getOrCreateRocks()->getOrCreateHandler();
            /// Deserializing left_input state
            {
                String left_input_str;
                rocks_handler->get("_left_input", left_input_str);
                ReadBufferFromString rb(left_input_str);
                left_input.deserialize(rb);
            }

            /// Deserializing right_input state
            {
                String right_input_str;
                rocks_handler->get("_right_input", right_input_str);
                ReadBufferFromString rb(right_input_str);
                right_input.deserialize(rb);
            }
            break;
        }
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) {
                /// Deserializing join algorithm state
                join->deserialize(rb, version_);

                /// Deserializing left_input state
                left_input.deserialize(rb);

                /// Deserializing right_input state
                right_input.deserialize(rb);
            });
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unknown checkpoint type {}", ckpt->type());
        }
    }

    /// Re-init last data ts
    left_input.last_data_ts = right_input.last_data_ts = DB::MonotonicMilliseconds::now();
}

void JoinTransformWithAlignment::InputPortWithData::serialize(WriteBuffer & wb) const
{
    if (need_buffer_data_to_align)
    {
        DB::writeVarUInt(input_chunks.size(), wb);
        const auto & header = input_port->getHeader();
        for (const auto & chunk : input_chunks)
            DB::writeLightChunkWithTimestamp(chunk, header, ProtonRevision::getVersionRevision(), wb);
    }
    else
    {
        /// Don't buffer input chunks and directly push to hash table, so we can assume no buffered data when received request checkpoint, ,
        assert(input_chunks.empty());
    }

    if (watermark_column_position)
        DB::writeIntBinary(watermark, wb);
}

void JoinTransformWithAlignment::InputPortWithData::deserialize(ReadBuffer & rb)
{
    if (need_buffer_data_to_align)
    {
        size_t size;
        DB::readVarUInt(size, rb);
        input_chunks.resize(size);
        const auto & header = input_port->getHeader();
        for (auto & chunk : input_chunks)
            chunk = DB::readLightChunkWithTimestamp(header, ProtonRevision::getVersionRevision(), rb);
    }

    if (watermark_column_position)
        DB::readIntBinary(watermark, rb);
}
}
}
