#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Core/BlockRangeSplitter.h>

#include <gtest/gtest.h>

namespace
{
DB::Block baseBlock()
{
    DB::Block block;

    auto uint64_type = std::make_shared<DB::DataTypeUInt64>();
    auto float64_type = std::make_shared<DB::DataTypeFloat64>();
    auto datetime64_type = std::make_shared<DB::DataTypeDateTime64>(3);
    auto string_type = std::make_shared<DB::DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto * id_col_inner = typeid_cast<DB::ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(102);
    id_col_inner->insertValue(101);
    id_col_inner->insertValue(100);

    DB::ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto * cpu_col_inner = typeid_cast<DB::ColumnFloat64 *>(cpu_col.get());
    cpu_col_inner->insertValue(13.338);
    cpu_col_inner->insertValue(17.378);
    cpu_col_inner->insertValue(11.539);

    DB::ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    auto raw_col = string_type->createColumn();
    raw_col->insertData("hello", 5);
    raw_col->insertData("world", 5);
    raw_col->insertData("你好啊", 6);

    DB::ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto * time_col_inner = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(time_col.get());
    time_col_inner->insertValue(1612286044256);
    time_col_inner->insertValue(1612296044256);
    time_col_inner->insertValue(1612276044256);

    DB::ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, "timestamp");
    block.insert(time_col_with_type);

    return block;
}

struct BucketInfo
{
    std::vector<UInt64> rows;
    Int64 min = std::numeric_limits<Int64>::max();
    Int64 max = std::numeric_limits<Int64>::min();
};

template <typename DataType, typename ColumnType, UInt32 scale, typename V>
std::unordered_map<Int64, BucketInfo> addSplitColumn(DB::Block & block, std::vector<V> values, UInt64 range)
{
    assert(block.rows() == values.size());

    DB::DataTypePtr col_type = nullptr;
    if constexpr (scale > 0)
        col_type = std::make_shared<DataType>(scale);
    else
        col_type = std::make_shared<DataType>();

    auto time_col = col_type->createColumn();
    auto * time_col_inner = typeid_cast<ColumnType *>(time_col.get());

    for (auto v : values)
        time_col_inner->insertValue(v);

    DB::ColumnWithTypeAndName time_col_with_type(std::move(time_col), std::move(col_type), "_tp_time");

    block.insert(time_col_with_type);

    /// bucket -> row positions
    std::unordered_map<Int64, BucketInfo> bucket_to_rows;

    for (size_t row = 0; auto v : values)
    {
        auto & bucket_info = bucket_to_rows[v / range * range];
        bucket_info.rows.push_back(row);
        bucket_info.min = std::min<Int64>(bucket_info.min, v);
        bucket_info.max = std::max<Int64>(bucket_info.max, v);

        ++row;
    }

    return bucket_to_rows;
}

template <typename DataType, typename ColumnType, UInt32 scale, typename V>
void testCommon(std::vector<V> values, UInt64 range = 10)
{
    DB::Block block{baseBlock()};

    auto bucket_to_rows{addSplitColumn<DataType, ColumnType, scale, V>(block, std::move(values), range)};

    auto position = block.getPositionByName("_tp_time");

    auto range_splitter{DB::createBlockRangeSplitter(block.getByPosition(position).type->getTypeId(), position, range, true)};
    auto results{range_splitter->split(block)};

    ASSERT_EQ(bucket_to_rows.size(), results.size());

    for (const auto & bucket_block : results)
    {
        ASSERT_TRUE(bucket_to_rows.contains(bucket_block.first));
        ASSERT_EQ(bucket_to_rows[bucket_block.first].rows.size(), bucket_block.second.rows());
        ASSERT_EQ(block.columns(), bucket_block.second.columns());

        auto bucket_info = bucket_to_rows[bucket_block.first];
        bucket_to_rows.erase(bucket_block.first);

        for (size_t col_pos = 0; const auto & col_with_type : bucket_block.second)
        {
            for (size_t new_row = 0; auto row : bucket_info.rows)
            {
                ASSERT_EQ(col_with_type.column->compareAt(new_row, row, *block.getByPosition(col_pos).column, -1), 0);
                ++new_row;
            }

            ++col_pos;
        }

        ASSERT_EQ(bucket_info.min, bucket_block.second.info.watermark_lower_bound);
        ASSERT_EQ(bucket_info.max, bucket_block.second.info.watermark);
    }
}
}

GTEST_TEST(SplitBlock, SameBucketDateTime64)
{
    std::vector<Int64> values = {{1652980026940, 1652980027940, 1652980028940}};
    testCommon<DB::DataTypeDateTime64, DB::ColumnDecimal<DB::DateTime64>, 3, Int64>(values, 10000);
}

GTEST_TEST(SplitBlock, DifferentBucketDateTime64)
{
    std::vector<Int64> values = {{1652980026940, 1652980037940, 1652980048940}};
    testCommon<DB::DataTypeDateTime64, DB::ColumnDecimal<DB::DateTime64>, 3, Int64>(values, 10000);
}

GTEST_TEST(SplitBlock, SameBucketDateTime)
{
    std::vector<UInt32> values = {{1652980026, 1652980027, 1652980028}};
    testCommon<DB::DataTypeDateTime, DB::ColumnVector<UInt32>, 0, UInt32>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketDateTime)
{
    std::vector<UInt32> values = {{1652980026, 1652980037, 1652980048}};
    testCommon<DB::DataTypeDateTime, DB::ColumnVector<UInt32>, 0, UInt32>(values);
}

GTEST_TEST(SplitBlock, SameBucketDate)
{
    std::vector<UInt16> values = {{8026, 8027, 8028}};
    testCommon<DB::DataTypeDate, DB::ColumnVector<UInt16>, 0, UInt16>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketDate)
{
    std::vector<UInt16> values = {{8026, 8037, 8048}};
    testCommon<DB::DataTypeDate, DB::ColumnVector<UInt16>, 0, UInt16>(values);
}

GTEST_TEST(SplitBlock, SameBucketDate32)
{
    std::vector<Int32> values = {{1652980026, 1652980027, 1652980028}};
    testCommon<DB::DataTypeDate32, DB::ColumnVector<Int32>, 0, Int32>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketDate32)
{
    std::vector<Int32> values = {{1652980026, 1652980037, 1652980048}};
    testCommon<DB::DataTypeDate32, DB::ColumnVector<Int32>, 0, Int32>(values);
}

GTEST_TEST(SplitBlock, SameBucketInt64)
{
    std::vector<Int64> values = {{1652980026940, 1652980027940, 1652980028940}};
    testCommon<DB::DataTypeInt64, DB::ColumnVector<Int64>, 0, Int64>(values, 10000);
}

GTEST_TEST(SplitBlock, DifferentBucketInt64)
{
    std::vector<Int64> values = {{1652980026940, 1652980037940, 1652980048940}};
    testCommon<DB::DataTypeInt64, DB::ColumnVector<Int64>, 0, Int64>(values, 10000);
}

GTEST_TEST(SplitBlock, SameBucketInt32)
{
    std::vector<Int32> values = {{1652980026, 1652980027, 1652980028}};
    testCommon<DB::DataTypeInt32, DB::ColumnVector<Int32>, 0, Int32>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketInt32)
{
    std::vector<Int32> values = {{1652980026, 1652980037, 1652980048}};
    testCommon<DB::DataTypeInt32, DB::ColumnVector<Int32>, 0, Int32>(values);
}

GTEST_TEST(SplitBlock, SameBucketInt16)
{
    std::vector<Int16> values = {{8026, 8027, 8028}};
    testCommon<DB::DataTypeInt16, DB::ColumnVector<Int16>, 0, Int16>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketInt16)
{
    std::vector<Int16> values = {{8026, 8037, 8048}};
    testCommon<DB::DataTypeInt16, DB::ColumnVector<Int16>, 0, Int16>(values);
}

GTEST_TEST(SplitBlock, SameBucketInt8)
{
    std::vector<Int8> values = {{26, 27, 28}};
    testCommon<DB::DataTypeInt8, DB::ColumnVector<Int8>, 0, Int8>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketInt8)
{
    std::vector<Int8> values = {{26, 37, 48}};
    testCommon<DB::DataTypeInt8, DB::ColumnVector<Int8>, 0, Int8>(values);
}

GTEST_TEST(SplitBlock, SameBucketUInt64)
{
    std::vector<UInt64> values = {{1652980026940, 1652980027940, 1652980028940}};
    testCommon<DB::DataTypeUInt64, DB::ColumnVector<UInt64>, 0, UInt64>(values, 10000);
}

GTEST_TEST(SplitBlock, DifferentBucketUInt64)
{
    std::vector<UInt64> values = {{1652980026940, 1652980037940, 1652980048940}};
    testCommon<DB::DataTypeUInt64, DB::ColumnVector<UInt64>, 0, UInt64>(values, 10000);
}

GTEST_TEST(SplitBlock, SameBucketUInt32)
{
    std::vector<UInt32> values = {{1652980026, 1652980027, 1652980028}};
    testCommon<DB::DataTypeUInt32, DB::ColumnVector<UInt32>, 0, UInt32>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketUInt32)
{
    std::vector<UInt32> values = {{1652980026, 1652980037, 1652980048}};
    testCommon<DB::DataTypeUInt32, DB::ColumnVector<UInt32>, 0, UInt32>(values);
}

GTEST_TEST(SplitBlock, SameBucketUInt16)
{
    std::vector<UInt16> values = {{8026, 8027, 8028}};
    testCommon<DB::DataTypeUInt16, DB::ColumnVector<UInt16>, 0, UInt16>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketUInt16)
{
    std::vector<UInt16> values = {{8026, 8037, 8048}};
    testCommon<DB::DataTypeUInt16, DB::ColumnVector<UInt16>, 0, UInt16>(values);
}

GTEST_TEST(SplitBlock, SameBucketUInt8)
{
    std::vector<UInt8> values = {{26, 27, 28}};
    testCommon<DB::DataTypeUInt8, DB::ColumnVector<UInt8>, 0, UInt8>(values);
}

GTEST_TEST(SplitBlock, DifferentBucketUInt8)
{
    std::vector<UInt8> values = {{26, 37, 48}};
    testCommon<DB::DataTypeUInt8, DB::ColumnVector<UInt8>, 0, UInt8>(values);
}
