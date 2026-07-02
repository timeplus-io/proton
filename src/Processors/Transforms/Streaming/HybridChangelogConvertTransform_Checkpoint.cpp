#include <Processors/Transforms/Streaming/HybridChangelogConvertTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
RocksDBPtr HybridChangelogConvertTransform::getOrCreateRocksDB(const HybridConfig & hybrid_config)
{
    /// Initialize rocks on first use
    if (!rocks)
        rocks = RocksDB::createOrLoadIfExists(
            hybrid_config.getRocksOptions(), hybrid_config.spill_dir_path, /*ttl=*/0, hybrid_config.cleanup_on_disk_data, logger);

    return rocks;
}

void HybridChangelogConvertTransform::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    auto & settings = ckpt_ctx->request_ctx->settings;
    CheckpointPtr ckpt;
    switch (settings->type)
    {
        case CheckpointType::Auto:
        case CheckpointType::Rocks:
        {
            chassert(!index.isTwoLevel());
            index.flush();
            getOrCreateRocksDB()->getDefaultColumnFamilyHandler()->put("late_rows", late_rows);
            ckpt = std::make_shared<RocksCheckpoint>(getVersion(), rocks);
            break;
        }
        case CheckpointType::File:
        {
            ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
                index.serialize(wb);
                DB::writeIntBinary(late_rows, wb);
            });
            break;
        }
    }

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void HybridChangelogConvertTransform::doRecover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([this](ReadBuffer & rb) {
                index.deserialize(rb);
                DB::readIntBinary(late_rows, rb);
            });
            break;
        }
        case CheckpointType::Rocks:
        {
            auto rocks_ckpt = std::static_pointer_cast<RocksCheckpoint>(ckpt);

            /// NOTE: All rocks handlers are invalid after shutdown
            if (rocks)
            {
                rocks->shutdown(/*cleanup=*/true);
                rocks.reset();
            }

            rocks_ckpt->recover(config.base_conf.spill_dir_path);

            /// Reinstall recovered rocks
            rocks = RocksDB::createOrLoadIfExists(
                config.getRocksOptions(), config.base_conf.spill_dir_path, /*ttl=*/0, config.base_conf.cleanup_on_disk_data, logger);
            rocks->getDefaultColumnFamilyHandler()->get("late_rows", late_rows);
            index.reload();
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unsupported checkpoint type: {}", ckpt->type());
        }
    }
}

}
}
