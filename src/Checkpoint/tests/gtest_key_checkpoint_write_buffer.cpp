#include <Checkpoint/KeyCheckpointWriteBuffer.h>
#include <Cluster/Common/Nulls.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>

#include <cstring>
#include <filesystem>
#include <system_error>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int OK;
extern const int NOT_A_LEADER;
}


TEST(KeyCheckpointWriteBufferTest, Base)
{
    SCOPE_EXIT({
        std::error_code ec;
        std::filesystem::remove_all("test_key.ckpt");
        std::filesystem::remove_all("test_key2.ckpt");
    });

    auto key1_file_buf = std::make_unique<DB::WriteBufferFromFile>("test_key.ckpt");
    auto key2_file_buf = std::make_unique<DB::WriteBufferFromFile>("test_key2.ckpt");
    DB::KeyCheckpointWriteBuffer write_buffer{};
    ASSERT_FALSE(write_buffer.initialized());
    write_buffer.init("key", 1, DB::CheckpointType::File, std::move(key1_file_buf));

    ASSERT_TRUE(write_buffer.append(0, "he"));

    ASSERT_TRUE(write_buffer.append(1, "llo"));

    /// Failed to append since seq_num should be consecutive
    ASSERT_FALSE(write_buffer.append(4, "ll"));

    ASSERT_TRUE(write_buffer.append(2, " world"));

    ASSERT_NO_THROW(write_buffer.finalize());

    /// Failed to append since already finalized
    ASSERT_FALSE(write_buffer.append(3, "!"));

    /// Check the content of the checkpoint file of key
    DB::ReadBufferFromFile read_buffer("test_key.ckpt");
    String data;
    DB::readString(data, read_buffer);
    ASSERT_EQ(data, String("hello world"));

    /// Reinit next key
    ASSERT_FALSE(write_buffer.initialized());
    write_buffer.init("key2", 1, DB::CheckpointType::File, std::move(key2_file_buf));

    ASSERT_TRUE(write_buffer.append(0, "test"));
    ASSERT_NO_THROW(write_buffer.finalize());

    /// Check the content of the checkpoint file of key2
    DB::ReadBufferFromFile read_buffer2("test_key2.ckpt");
    String data2;
    DB::readString(data2, read_buffer2);
    ASSERT_EQ(data2, String("test"));
}
