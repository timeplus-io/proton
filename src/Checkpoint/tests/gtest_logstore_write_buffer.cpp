#include <Cluster/Common/Nulls.h>
#include <Common/Exception.h>

/// Enable UT flush implementation
#define ENABLE_UT_FLUSH
#include <Checkpoint/LogStoreWriteBuffer.cpp> // NOLINT

#include <cstring>

#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int OK;
extern const int NOT_A_LEADER;
}

namespace
{
const cluster::StreamShard test_stream_shard{"ns", "test", cluster::Nulls::NullStreamID, 1};
}

TEST(LogStoreWriteBufferTest, WholeFlush)
{
    DB::LogStoreWriteBuffer buffer(test_stream_shard, 16);
    buffer.setPrefixFlagGetter([&](uint32_t seq_num, bool is_last_segment) -> uint64_t {
        EXPECT_EQ(seq_num, 0);
        EXPECT_TRUE(is_last_segment);
        return 1;
    });

    String str("hello");

    buffer.setFlushFunc([&](auto && data) -> int {
        /// header + str
        EXPECT_EQ(data.size(), sizeof(uint64_t) + str.size());
        EXPECT_EQ(*reinterpret_cast<const int64_t *>(data.data()), 1);
        EXPECT_EQ(std::string_view(data.data() + sizeof(uint64_t), str.size()), std::string_view(str.data(), str.size()));
        return DB::ErrorCodes::OK;
    });

    buffer.write(str.data(), str.size());
    EXPECT_NO_THROW(buffer.finalize());
}

TEST(LogStoreWriteBufferTest, WholeFlushError)
{
    DB::LogStoreWriteBuffer buffer(test_stream_shard, 16);
    buffer.setPrefixFlagGetter([&](uint32_t seq_num, bool is_last_segment) -> uint64_t {
        EXPECT_EQ(seq_num, 0);
        EXPECT_TRUE(is_last_segment);
        return 1;
    });

    String str("hello");

    buffer.setFlushFunc([&](auto && data) -> int {
        /// header + str
        EXPECT_EQ(data.size(), sizeof(uint64_t) + str.size());
        EXPECT_EQ(*reinterpret_cast<const int64_t *>(data.data()), 1);
        EXPECT_EQ(std::string_view(data.data() + sizeof(uint64_t), str.size()), std::string_view(str.data(), str.size()));
        return DB::ErrorCodes::NOT_A_LEADER;
    });

    buffer.write(str.data(), str.size());
    EXPECT_THROW(buffer.finalize(), DB::Exception);
}

TEST(LogStoreWriteBufferTest, SegmentFlush)
{
    std::vector<std::pair<uint32_t, bool>> segments;

    /// For each segment, we only 1 bytes payload
    size_t max_segment_size = sizeof(uint64_t) + 1;
    DB::LogStoreWriteBuffer buffer(test_stream_shard, max_segment_size);
    buffer.setPrefixFlagGetter([&](uint32_t seq_num, bool is_last_segment) -> uint64_t {
        segments.emplace_back(seq_num, is_last_segment);
        return (static_cast<uint64_t>(seq_num) << 32) + is_last_segment;
    });

    String str("hello");

    String res("");

    buffer.setFlushFunc([&](auto && data) -> int {
        EXPECT_EQ(data.size(), max_segment_size);
        res += data[sizeof(uint64_t)];
        return DB::ErrorCodes::OK;
    });

    buffer.write(str.data(), str.size());
    ASSERT_EQ(segments.size(), 4);
    EXPECT_EQ(segments.at(0), std::make_pair(static_cast<uint32_t>(0), false));
    EXPECT_EQ(segments.at(1), std::make_pair(static_cast<uint32_t>(1), false));
    EXPECT_EQ(segments.at(2), std::make_pair(static_cast<uint32_t>(2), false));
    EXPECT_EQ(segments.at(3), std::make_pair(static_cast<uint32_t>(3), false));
    EXPECT_EQ(res, "hell");

    /// Flush buffer
    ASSERT_NO_THROW(buffer.finalize());
    ASSERT_EQ(segments.size(), 5);
    EXPECT_EQ(segments.at(4), std::make_pair(static_cast<uint32_t>(4), true));
    EXPECT_EQ(res, "hello");
}
