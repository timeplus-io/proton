#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Streaming/SizedChunkSplitter.h>

#include <gtest/gtest.h>


using namespace DB;
using namespace DB::Streaming;

TEST(SizedChunkSplitter, SplitSingleShardByMaxSize)
{
    SizedChunkSplitter splitter(3, 1024 * 1024); // 3 rows, 1MB

    Block block;
    auto data_type = std::make_shared<DataTypeString>();
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnString *>(col.get());
    for (size_t i = 0; i < 10; ++i)
        col_ptr->insert(std::to_string(i + 100));
    block.insert(ColumnWithTypeAndName(std::move(col), data_type, "name"));

    auto result = splitter.splitBlockForSingleShard(block, 42);
    ASSERT_EQ(result.size(), 4);
    ASSERT_EQ(result[0].block.rows(), 3);
    ASSERT_EQ(result[1].block.rows(), 3);
    ASSERT_EQ(result[2].block.rows(), 3);
    ASSERT_EQ(result[3].block.rows(), 1);
    ASSERT_EQ(result[0].shard, 42);
    ASSERT_EQ(result[1].shard, 42);
    ASSERT_EQ(result[2].shard, 42);
    ASSERT_EQ(result[3].shard, 42);
}

TEST(SizedChunkSplitter, SplitSingleShardByMaxBytes)
{
    SizedChunkSplitter splitter(1000000, 24); // 1000000 rows, 24 bytes

    Block block;
    auto data_type = std::make_shared<DataTypeInt64>();
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnInt64 *>(col.get());
    for (size_t i = 0; i < 1000; ++i)
        col_ptr->insert(i);
    block.insert(ColumnWithTypeAndName(std::move(col), data_type, "number"));

    auto result = splitter.splitBlockForSingleShard(block, 24);
    ASSERT_EQ(result.size(), 1000 * 8 / 24 + 1);
    ASSERT_EQ(result[0].block.rows(), 3);
    ASSERT_EQ(result[0].shard, 24);
    ASSERT_EQ(result[1].block.rows(), 3);
    ASSERT_EQ(result[1].shard, 24);
    ASSERT_EQ(result.back().block.rows(), 1);
    ASSERT_EQ(result.back().shard, 24);
}

TEST(SizedChunkSplitter, SplitMultipleShardsByMaxSize)
{
    SizedChunkSplitter splitter(3, 10000); // 3 rows, 10000 bytes

    /// Block with two columns and 1,000 rows; each row is 16 bytes.
    Block block;
    auto data_type = std::make_shared<DataTypeInt64>();
    auto col1 = data_type->createColumn();
    auto * col1_ptr = typeid_cast<ColumnInt64 *>(col1.get());
    auto col2 = data_type->createColumn();
    auto * col2_ptr = typeid_cast<ColumnInt64 *>(col2.get());
    for (size_t i = 0; i < 1000; ++i)
    {
        col1_ptr->insert(i);
        col2_ptr->insert(i + 1234);
    }
    block.insert(ColumnWithTypeAndName(std::move(col1), data_type, "number1"));
    block.insert(ColumnWithTypeAndName(std::move(col2), data_type, "number2"));

    IColumn::Selector selector(1000);
    size_t i = 0;
    while (i < 1000)
    {
        selector[i++] = 1;
        selector[i++] = 2;
        selector[i++] = 0;
        selector[i++] = 0;
        selector[i++] = 1;
    }

    auto result = splitter.splitBlock(block, 3, selector);
    ASSERT_EQ(result.size(), 335);
    ASSERT_EQ(selector[0], 0);
    ASSERT_EQ(selector[1], 1);
    ASSERT_EQ(selector[2], 2);
    ASSERT_EQ(selector[4], 0);
    ASSERT_EQ(selector[8], 3);
    ASSERT_EQ(selector[9], 4);

    std::vector<int> shard_counts(3, 0);
    for (const auto & chunk : result)
        ++shard_counts[chunk.shard];
    ASSERT_EQ(shard_counts[0], 134);
    ASSERT_EQ(shard_counts[1], 134);
    ASSERT_EQ(shard_counts[2], 67);
}

TEST(SizedChunkSplitter, SplitMultipleShardsByMaxBytes)
{
    SizedChunkSplitter splitter(1000000, 48); // 1000000 rows, 48 bytes

    /// Block with two columns and 1,000 rows; each row is 16 bytes.
    Block block;
    auto data_type = std::make_shared<DataTypeInt64>();
    auto col1 = data_type->createColumn();
    auto * col1_ptr = typeid_cast<ColumnInt64 *>(col1.get());
    auto col2 = data_type->createColumn();
    auto * col2_ptr = typeid_cast<ColumnInt64 *>(col2.get());
    for (size_t i = 0; i < 1000; ++i)
    {
        col1_ptr->insert(i);
        col2_ptr->insert(i + 1234);
    }
    block.insert(ColumnWithTypeAndName(std::move(col1), data_type, "number1"));
    block.insert(ColumnWithTypeAndName(std::move(col2), data_type, "number2"));

    IColumn::Selector selector(1000);
    size_t i = 0;
    while (i < 1000)
    {
        selector[i++] = 1;
        selector[i++] = 2;
        selector[i++] = 0;
        selector[i++] = 0;
        selector[i++] = 1;
    }

    auto result = splitter.splitBlock(block, 3, selector);
    ASSERT_EQ(result.size(), 335);
    ASSERT_EQ(selector[0], 0);
    ASSERT_EQ(selector[1], 1);
    ASSERT_EQ(selector[2], 2);
    ASSERT_EQ(selector[4], 0);
    ASSERT_EQ(selector[8], 3);
    ASSERT_EQ(selector[9], 4);

    std::vector<int> shard_counts(3, 0);
    for (const auto & chunk : result)
        ++shard_counts[chunk.shard];
    ASSERT_EQ(shard_counts[0], 134);
    ASSERT_EQ(shard_counts[1], 134);
    ASSERT_EQ(shard_counts[2], 67);
}
