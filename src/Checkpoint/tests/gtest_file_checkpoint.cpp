#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>

#include <cstring>
#include <filesystem>
#include <system_error>

#include <gtest/gtest.h>

namespace DB
{
class CheckpointCoordinator;
}

using namespace DB;

TEST(FileCheckpoint, Basic)
{
    std::string base_dir = "test_file_ckpt";
    std::filesystem::remove_all(base_dir);
    SCOPE_EXIT({ std::filesystem::remove_all(base_dir); });

    LocalFileSystemCheckpointStorage local_fs_ckpt_storage(base_dir);
    std::string qid = "qid_1";

    auto ckpt_ctx
        = std::make_shared<CheckpointContext>(qid, local_fs_ckpt_storage, /*no_use*/ reinterpret_cast<CheckpointCoordinator *>(0x1));
    local_fs_ckpt_storage.preCheckpoint(ckpt_ctx); /// Epoch 0 to create checkpoint dir

    /// Epoch 1
    ckpt_ctx->epoch = 1;
    local_fs_ckpt_storage.preCheckpoint(ckpt_ctx);
    CheckpointPtr ckpt = std::make_shared<FileCheckpoint>(1, [](WriteBuffer & wb) { writeStringBinary("hello world!", wb); });
    local_fs_ckpt_storage.checkpoint("key1", std::move(ckpt), ckpt_ctx);
    local_fs_ckpt_storage.commit(ckpt_ctx);
    ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx), 1);

    ckpt = local_fs_ckpt_storage.recover("key1", ckpt_ctx);
    ASSERT_EQ(ckpt->getVersion(), 1);
    ASSERT_EQ(ckpt->type(), CheckpointType::File);
    std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([](ReadBuffer & rb) {
        String data;
        readStringBinary(data, rb);
        ASSERT_EQ(data, String("hello world!"));
    });

    /// Epoch2 (No commit) (Compressed)
    ckpt_ctx->epoch = 2;
    local_fs_ckpt_storage.preCheckpoint(ckpt_ctx);
    ckpt = std::make_shared<FileCheckpoint>(
        2,
        [](WriteBuffer & wb) {
            CompressedWriteBuffer compressed_wb(wb);
            writeStringBinary("happy!", compressed_wb);
            compressed_wb.finalize();
        },
        /*compressed=*/true);
    local_fs_ckpt_storage.checkpoint("key2", std::move(ckpt), ckpt_ctx);

    ckpt = local_fs_ckpt_storage.recover("key2", ckpt_ctx);
    ASSERT_EQ(ckpt->getVersion(), 2);
    ASSERT_EQ(ckpt->type(), CheckpointType::File);
    std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([](ReadBuffer & rb) {
        String data;
        readStringBinary(data, rb);
        ASSERT_EQ(data, String("happy!"));
    });

    /// Epoch3
    ckpt_ctx->epoch = 3;
    local_fs_ckpt_storage.preCheckpoint(ckpt_ctx);
    local_fs_ckpt_storage.commit(ckpt_ctx);
    ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx), 3);

    /// Remove all ckpt before epoch 3
    ckpt_ctx->epoch = 3;
    local_fs_ckpt_storage.remove(ckpt_ctx);
    ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx), 3);

    /// Epoch 1 was removed
    ckpt_ctx->epoch = 1;
    ASSERT_FALSE(local_fs_ckpt_storage.checkpointDirExists(ckpt_ctx));

    /// Remove all
    ckpt_ctx->epoch = 0;
    local_fs_ckpt_storage.remove(ckpt_ctx);
    ASSERT_EQ(local_fs_ckpt_storage.getLastCommittedEpoch(ckpt_ctx), 0);
}

TEST(FileCheckpoint, Serder)
{
    std::string base_dir = "test_file_ckpt";
    std::filesystem::remove_all(base_dir);
    SCOPE_EXIT({ std::filesystem::remove_all(base_dir); });

    /// Case-1: Serialize and deserialize
    {
        CheckpointPtr ckpt = std::make_shared<FileCheckpoint>(1, [](WriteBuffer & wb) { writeStringBinary("hello world!", wb); });

        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/false);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/false);
        ASSERT_EQ(ckpt->getVersion(), 1);
        ASSERT_EQ(ckpt->type(), CheckpointType::File);
        std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([](ReadBuffer & rb) {
            String data;
            readStringBinary(data, rb);
            ASSERT_EQ(data, String("hello world!"));
        });
    }

    /// Case-2: Serialize and deserialize with compressed
    {
        CheckpointPtr ckpt = std::make_shared<FileCheckpoint>(2, [](WriteBuffer & wb) { writeStringBinary("happy!", wb); });

        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/true);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/true);
        ASSERT_EQ(ckpt->getVersion(), 2);
        ASSERT_EQ(ckpt->type(), CheckpointType::File);
        std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([](ReadBuffer & rb) {
            String data;
            readStringBinary(data, rb);
            ASSERT_EQ(data, String("happy!"));
        });
    }

    /// Case-3: Serialize and deserialize with compressed (Variant)
    {
        CheckpointPtr ckpt = std::make_shared<FileCheckpoint>(
            3,
            [](WriteBuffer & wb) {
                CompressedWriteBuffer compressed_wb(wb);
                writeStringBinary("happy!", compressed_wb);
                compressed_wb.finalize();
            },
            /*compressed=*/true);

        WriteBufferFromOwnString wb;
        ckpt->serialize(wb, /*compress_entity=*/true);
        ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(wb.str()), /*entity_compressed=*/true);
        ASSERT_EQ(ckpt->getVersion(), 3);
        ASSERT_EQ(ckpt->type(), CheckpointType::File);
        std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([](ReadBuffer & rb) {
            String data;
            readStringBinary(data, rb);
            ASSERT_EQ(data, String("happy!"));
        });
    }
}
