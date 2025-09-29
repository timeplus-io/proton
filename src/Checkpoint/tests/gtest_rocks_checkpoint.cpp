#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>
#include <Checkpoint/RocksCheckpoint.h>
#include <base/scope_guard.h>
#include <Common/Rocks/RocksHandler.h>

#include <cstring>
#include <filesystem>
#include <system_error>

#include <gtest/gtest.h>

namespace DB
{
class CheckpointCoordinator;
}

using namespace std::literals;
using namespace DB;

TEST(RocksCheckpoint, Basic)
{
    std::filesystem::path base_dir = "test_rocks_ckpt";
    std::filesystem::remove_all(base_dir);

    LocalFileSystemCheckpointStorage local_fs_ckpt_storage(base_dir / "ckpt_storage");

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    auto rocks = Rocks::createOrLoadIfExists(options, base_dir / "rocks", /*cleanup=*/false);
    auto rocks_handler = rocks->getOrCreateHandler();
    auto rocks_handler2 = rocks->getOrCreateHandler("another");
    rocks_handler->put("k1"sv, "v1"sv);
    rocks_handler2->put("k2"sv, "v2"sv);

    auto ckpt_ctx
        = std::make_shared<CheckpointContext>("qid_1", local_fs_ckpt_storage, /*no_use*/ reinterpret_cast<CheckpointCoordinator *>(0x1));
    local_fs_ckpt_storage.preCheckpoint(ckpt_ctx); /// Epoch 0 to create checkpoint dir

    auto verify_case = [&](CheckpointPtr ckpt, Int64 epoch) {
        ckpt_ctx->epoch = CheckpointEpoch{.epoch = epoch};

        local_fs_ckpt_storage.preCheckpoint(ckpt_ctx);
        local_fs_ckpt_storage.checkpoint("key", std::move(ckpt), ckpt_ctx);
        local_fs_ckpt_storage.commit(ckpt_ctx);
        ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx).epoch, epoch);

        auto recovered_rocks_path = base_dir / fmt::format("recovered_rocks{}", epoch);
        auto recovered_ckpt = local_fs_ckpt_storage.recover("key", ckpt_ctx);
        ASSERT_EQ(recovered_ckpt->type(), CheckpointType::Rocks);
        std::static_pointer_cast<RocksCheckpoint>(recovered_ckpt)->recover(recovered_rocks_path);
        rocks = Rocks::createOrLoadIfExists(options, recovered_rocks_path);
        rocks_handler = rocks->getOrCreateHandler();
        rocks_handler2 = rocks->getOrCreateHandler("another");

        std::string value;
        rocks_handler->get("k1"sv, value);
        ASSERT_EQ(value, "v1"sv);
        rocks_handler2->get("k2"sv, value);
        ASSERT_EQ(value, "v2"sv);
    };

    /// Case-1: Checkpoint/Recover rocks checkpoint directly (Epoch-1)
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(1, rocks);
        verify_case(ckpt, 1);
    }

    /// Case-2: Checkpoint/Recover rocks checkpoint after serialized/deserialized (Epoch-2)
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(2, rocks);
        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/false);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/false);
        ASSERT_EQ(ckpt->getVersion(), 2);
        verify_case(ckpt, 2);
    }

    /// Case-3: Checkpoint/Recover rocks checkpoint after serialized/deserialized with compressed (Epoch-2)
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(3, rocks);
        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/true);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/true);
        ASSERT_EQ(ckpt->getVersion(), 3);
        verify_case(ckpt, 3);
    }
}

TEST(RocksCheckpoint, Incremental)
{
    std::filesystem::path base_dir = "test_rocks_ckpt";
    SCOPE_EXIT({ std::filesystem::remove_all(base_dir); });

    LocalFileSystemCheckpointStorage local_fs_ckpt_storage(base_dir / "ckpt_storage");

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    auto rocks = Rocks::createOrLoadIfExists(options, base_dir / "rocks");
    auto rocks_handler = rocks->getOrCreateHandler();
    auto rocks_handler2 = rocks->getOrCreateHandler("another");

    /// Updated k1 and added k2 in default handler
    rocks_handler->put("k1"sv, "vv1"sv);
    rocks_handler->put("k2"sv, "v2"sv);

    /// Added k1 and remove k2 in another handler
    rocks_handler2->put("k1"sv, "v1"sv);
    rocks_handler2->remove("k2"sv);

    auto ckpt_ctx
        = std::make_shared<CheckpointContext>("qid_1", local_fs_ckpt_storage, /*no_use*/ reinterpret_cast<CheckpointCoordinator *>(0x1));

    auto verify_case = [&](CheckpointPtr ckpt, Int64 epoch) {
        ckpt_ctx->epoch = CheckpointEpoch{.epoch = epoch};
        local_fs_ckpt_storage.preCheckpoint(ckpt_ctx);
        local_fs_ckpt_storage.checkpoint("key", std::move(ckpt), ckpt_ctx);
        local_fs_ckpt_storage.commit(ckpt_ctx);
        ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx).epoch, epoch);

        auto recovered_rocks_path = base_dir / fmt::format("recovered_rocks{}", epoch);
        auto recovered_ckpt = local_fs_ckpt_storage.recover("key", ckpt_ctx);
        ASSERT_EQ(recovered_ckpt->type(), CheckpointType::Rocks);
        std::static_pointer_cast<RocksCheckpoint>(recovered_ckpt)->recover(recovered_rocks_path);
        rocks = Rocks::createOrLoadIfExists(options, recovered_rocks_path);
        rocks_handler = rocks->getOrCreateHandler();
        rocks_handler2 = rocks->getOrCreateHandler("another");

        std::string value;
        rocks_handler->get("k1"sv, value);
        ASSERT_EQ(value, "vv1"sv);
        rocks_handler->get("k2"sv, value);
        ASSERT_EQ(value, "v2"sv);
        rocks_handler2->get("k1"sv, value);
        ASSERT_EQ(value, "v1"sv);
        ASSERT_FALSE(rocks_handler2->tryGet("k2"sv, value));
    };

    /// Case-4: Checkpoint/Recover incremental rocks ckpt directly
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(1, rocks);

        ckpt_ctx->epoch = CheckpointEpoch{.epoch = 3}; /// prev epoch before changing
        ASSERT_TRUE(ckpt->enableIncremental(local_fs_ckpt_storage.recover("key", ckpt_ctx)));
        verify_case(ckpt, 4);
    }

    /// Case-5: Checkpoint/Recover incremental rocks ckpt after serialized/deserialized
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(1, rocks);

        ckpt_ctx->epoch = CheckpointEpoch{.epoch = 4}; /// prev epoch before changing
        auto prev_ckpt = local_fs_ckpt_storage.recover("key", ckpt_ctx);
        ASSERT_TRUE(ckpt->enableIncremental(prev_ckpt));

        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/false);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/false);
        ASSERT_TRUE(ckpt->enableIncremental(prev_ckpt));
        verify_case(ckpt, 5);
    }

    /// Case-6: Checkpoint/Recover incremental rocks ckpt after serialized/deserialized with compressed
    {
        CheckpointPtr ckpt = std::make_shared<RocksCheckpoint>(1, rocks);

        ckpt_ctx->epoch = CheckpointEpoch{.epoch = 5}; /// prev epoch before changing
        auto prev_ckpt = local_fs_ckpt_storage.recover("key", ckpt_ctx);
        ASSERT_TRUE(ckpt->enableIncremental(prev_ckpt));

        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/true);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/true);
        ASSERT_TRUE(ckpt->enableIncremental(prev_ckpt));
        verify_case(ckpt, 6);
    }
}
